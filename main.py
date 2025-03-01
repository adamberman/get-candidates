import json
import requests
from datetime import datetime
import os
from typing import List, Dict
import base64
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import time



GREENHOUSE_BASE_URL = "https://harvest.greenhouse.io/v1"

def get_greenhouse_auth_headers(api_token: str) -> Dict:
    """
    Creates authentication headers for Greenhouse API requests.
    
    Args:
        api_token: Greenhouse API token
        
    Returns:
        Dictionary containing authorization headers
    """
    auth_token = base64.b64encode(f"{api_token}:".encode()).decode()
    
    return {
        "Authorization": f"Basic {auth_token}",
        "Content-Type": "application/json"
    }

def get_greenhouse_accepted_offers(api_token: str, start_date: str = "2023-07-01") -> List[Dict]:
    """
    Fetches all accepted offers from Greenhouse with starts_at date on or after the given start date.
    
    Args:
        api_token: Greenhouse API token
        start_date: Start date in YYYY-MM-DD format (default: 2023-07-01)
        
    Returns:
        List of accepted offer dictionaries
    """
    headers = get_greenhouse_auth_headers(api_token)
    accepted_offers = []
    page = 1
    
    while True:
        print(f"Fetching accepted offers page {page}")
        response = requests.get(
            f"{GREENHOUSE_BASE_URL}/offers",
            headers=headers,
            params={
                "per_page": 500,
                "page": page,
                "starts_after": start_date,
                "status": "accepted"
            }
        )
        response.raise_for_status()
        offers = response.json()
        if not offers:
            break
        accepted_offers.extend(offers)
        page += 1
        
    return accepted_offers


def get_greenhouse_scorecards(api_token: str, application_id: int) -> List[Dict]:
    """
    Fetches all scorecards for a given application id with rate limit handling.
    """
    print(f"Fetching scorecards for application {application_id}")
    headers = get_greenhouse_auth_headers(api_token)
    
    response = requests.get(
        f"{GREENHOUSE_BASE_URL}/applications/{application_id}/scorecards",
        headers=headers,
    )
    
    # Handle rate limits explicitly
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 10))
        print(f"Rate limited. Waiting {retry_after} seconds...")
        time.sleep(retry_after)
        return get_greenhouse_scorecards(api_token, application_id)
    
    response.raise_for_status()
    scorecards = response.json()
    return scorecards if scorecards else []

def get_greenhouse_candidates(api_token: str, candidate_ids: List[int]) -> Dict:
    """
    Fetches all candidates for a given list of candidate ids.
    
    Args:
        api_token: Greenhouse API token
        candidate_ids: List of candidate ids
    """
    headers = get_greenhouse_auth_headers(api_token)
    # Break candidate_ids into chunks of 50
    chunked_candidate_ids = [candidate_ids[i:i + 50] for i in range(0, len(candidate_ids), 50)]
    candidates = []
    i = 0
    for chunk_of_candidate_ids in chunked_candidate_ids:
        i += 1
        page = 1
        while True:
            print(f"Fetching candidates page {page} for chunk {i}")
            response = requests.get(
                f"{GREENHOUSE_BASE_URL}/candidates",
                headers=headers,
                params={
                    "page": page,
                    "per_page": 500,
                    "skip_count": True,
                    "candidate_ids": ",".join(map(str, chunk_of_candidate_ids))
                }
            )
            response.raise_for_status()
            new_candidates = response.json()
            if not new_candidates:
                break
            candidates.extend(new_candidates)
            page += 1
    return candidates

def format_scorecard(scorecard: Dict) -> Dict:
    return {
        "created_at": scorecard["created_at"],
        "interview": scorecard["interview"],
        "interview_step": scorecard["interview_step"]["name"] if "name" in scorecard["interview_step"] else None,
        "submitted_by": scorecard["submitted_by"]["name"] if "name" in scorecard["submitted_by"] else None,
        "interviewer": scorecard["interviewer"]["name"] if "name" in scorecard["interviewer"] else None,
        "overall_recommendation": scorecard["overall_recommendation"],
        "attributes": scorecard["attributes"],
        "ratings": scorecard["ratings"],
        "questions": [{ "question": question["question"], "answer": question["answer"] } for question in scorecard["questions"]],
    }

if __name__ == "__main__":
    api_token = os.getenv("GREENHOUSE_API_TOKEN")
    accepted_offers = get_greenhouse_accepted_offers(api_token)
    print(f"Found {len(accepted_offers)} accepted offers")
    scorecard_data = {}
    candidate_data = {}
    
    # Function to process a single offer
    def process_offer(application_id: int):
        return application_id, get_greenhouse_scorecards(api_token, application_id)
    
    # Use ThreadPoolExecutor to parallelize API calls
    with ThreadPoolExecutor(max_workers=6) as executor:
        print("Processing offers in parallel...")
        future_to_offer = {executor.submit(process_offer, offer["application_id"]): offer for offer in accepted_offers}
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_offer):
            completed += 1
            if completed % 10 == 0:
                print(f"Processed {completed} of {len(accepted_offers)} offers")
            try:
                application_id, scorecards = future.result()
                scorecard_data[application_id] = scorecards
            except Exception as exc:
                print(f'Offer processing generated an exception: {exc}')
    
    candidate_ids = [offer["candidate_id"] for offer in accepted_offers]
    candidates = get_greenhouse_candidates(api_token, candidate_ids)
    for candidate in candidates:
        candidate_data[candidate["id"]] = candidate
    print(f"Found {len(scorecard_data)} scorecards and {len(candidate_data)} candidates")
    candidates_to_offers = {}
    for offer in accepted_offers:
        if offer["candidate_id"] not in candidates_to_offers:
            candidate_name = candidate_data[offer["candidate_id"]]["first_name"] + " " + candidate_data[offer["candidate_id"]]["last_name"]
            candidates_to_offers[offer["candidate_id"]] = { "name": candidate_name, "offers": 1}
        else:
            candidates_to_offers[offer["candidate_id"]]["offers"] += 1
    for candidate_id, data in candidates_to_offers.items():
        if data["offers"] > 1:
            print(f"{data['name']} has {data['offers']} offers")

    # Collate all the data
    scorecards_data_for_offers = {}
    for offer in accepted_offers:
        candidate = candidate_data[offer["candidate_id"]]
        scorecards = scorecard_data[offer["application_id"]]
        application = next(app for app in candidate["applications"] if app["id"] == offer["application_id"])
        scorecards_data_for_offers[offer["id"]] = {
            "candidate_name": candidate["first_name"] + " " + candidate["last_name"],
            "candidate_data": {
                "company": candidate["company"],
                "title": candidate["title"],
                "created_at": candidate["created_at"],
                "recruiter_name": candidate["recruiter"]["name"] if "name" in candidate["recruiter"] else None,
            },
            "application_id": offer["application_id"],
            "application_data": {
                "source": application["source"]["public_name"] if "public_name" in application["source"] else None,
                "credited_to": application["credited_to"]["name"] if application["credited_to"] else None,
                "jobs": [job["name"] for job in application["jobs"]],
                "prospective_department": application["prospective_department"]
            },
            "scorecards_data": [format_scorecard(scorecard) for scorecard in scorecards]
        }
    
    # Write the data to a JSON file
    with open("scorecards_data.json", "w") as f:
        json.dump(scorecards_data_for_offers, f)
        
