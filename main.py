import argparse
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

def get_greenhouse_applications(api_token: str) -> List[Dict]:
    headers = get_greenhouse_auth_headers(api_token)
    applications = []
    page = 1
    while True:
        response = requests.get(
            f"{GREENHOUSE_BASE_URL}/applications",
            headers=headers,
            params={
                "page": page,
                "per_page": 500,
                "skip_count": True,
                "status": "hired"
            }
        )
        response.raise_for_status()
        new_applications = response.json()
        if not new_applications:
            break
        applications.extend(new_applications)
        page += 1
    return applications

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

def get_culture_amp_token(client_id: str, client_secret: str) -> str:
    response = requests.post(
        "https://api.cultureamp.com/v1/oauth2/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "target-entity:8ed17dce-9eca-4383-a9e1-54f82c362b6d:employees-read,performance-evaluations-read"
        }
    )
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_culture_amp_employees(client_id: str, client_secret: str) -> Dict:
    access_token = get_culture_amp_token(client_id, client_secret)
    employees = []
    pagination = ""
    while True:
        response = requests.get(
            f"https://api.cultureamp.com/v1/employees{pagination}",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        response.raise_for_status()
        data = response.json()
        employees.extend(data["employees"])
        if "pagination" in data and "afterKey" in data["pagination"]:
            pagination = f"?afterKey={data['pagination']['afterKey']}"
        else:
            break

    return employees

def format_scorecard(scorecard: Dict) -> Dict:
    return {
        "created_at": scorecard["created_at"],
        "interview": scorecard["interview"],
        "interview_step": scorecard["interview_step"]["name"] if "interview_step" in scorecard and scorecard["interview_step"] is not None else None,
        "submitted_by": scorecard["submitted_by"]["name"] if "submitted_by" in scorecard and scorecard["submitted_by"] is not None else None,
        "interviewer": scorecard["interviewer"]["name"] if "interviewer" in scorecard and scorecard["interviewer"] is not None else None,
        "overall_recommendation": scorecard["overall_recommendation"],
        "attributes": scorecard["attributes"],
        "ratings": scorecard["ratings"],
        "questions": [{ "question": question["question"], "answer": question["answer"] } for question in scorecard["questions"]],
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-applications", action="store_true", help="Use applications data")
    parser.add_argument("--skip-greenhouse", action="store_true", help="Skip greenhouse data")
    args = parser.parse_args()
    use_applications = args.use_applications
    skip_greenhouse = args.skip_greenhouse

    api_token = os.getenv("GREENHOUSE_API_TOKEN")
    culture_amp_client_id = os.getenv("CULTURE_AMP_CLIENT_ID")
    culture_amp_client_secret = os.getenv("CULTURE_AMP_CLIENT_SECRET")

    if not skip_greenhouse:
        job_dicts = []
        if use_applications:
            applications = get_greenhouse_applications(api_token)
            print(f"Found {len(applications)} applications")
            job_dicts = [{"application_id": application["id"], "candidate_id": application["candidate_id"]} for application in applications]
        else:
            accepted_offers = get_greenhouse_accepted_offers(api_token)
            print(f"Found {len(accepted_offers)} accepted offers")
            job_dicts = [{"application_id": offer["application_id"], "candidate_id": offer["candidate_id"]} for offer in accepted_offers]
        scorecard_data = {}
        candidate_data = {}
        
        # Function to process a single offer
        def process_offer(application_id: int):
            return application_id, get_greenhouse_scorecards(api_token, application_id)
        
        # Use ThreadPoolExecutor to parallelize API calls
        with ThreadPoolExecutor(max_workers=5) as executor:
            print("Processing offers in parallel...")
            future_to_offer = {executor.submit(process_offer, job_dict["application_id"]): job_dict for job_dict in job_dicts}
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_offer):
                completed += 1
                if completed % 10 == 0:
                    print(f"Processed {completed} of {len(job_dicts)} offers")
                try:
                    application_id, scorecards = future.result()
                    scorecard_data[application_id] = scorecards
                except Exception as exc:
                    print(f'Offer processing generated an exception: {exc}')
        
        candidate_ids = [job_dict["candidate_id"] for job_dict in job_dicts]
        candidates = get_greenhouse_candidates(api_token, candidate_ids)
        for candidate in candidates:
            candidate_data[candidate["id"]] = candidate
        print(f"Found {len(scorecard_data)} scorecards and {len(candidate_data)} candidates")
        candidates_to_offers = {}
        for candidate_id in candidate_ids:
            if candidate_id not in candidates_to_offers:
                candidate_name = candidate_data[candidate_id]["first_name"] + " " + candidate_data[candidate_id]["last_name"]
                candidates_to_offers[candidate_id] = { "name": candidate_name, "offers": 1}
            else:
                candidates_to_offers[candidate_id]["offers"] += 1
        for candidate_id, data in candidates_to_offers.items():
            if data["offers"] > 1:
                print(f"{data['name']} has {data['offers']} offers")
            else:
                print(f"{data['name']}")

    # fetch data from culture amp
    culture_amp_employees = fetch_culture_amp_employees(culture_amp_client_id, culture_amp_client_secret)
    print(f"Found {len(culture_amp_employees)} culture amp employees")

    if not skip_greenhouse:
    # Collate all the data
        scorecards_data_for_offers = {}
        for offer in job_dicts:
            candidate = candidate_data[offer["candidate_id"]]
            scorecards = scorecard_data[offer["application_id"]]
            application = next(app for app in candidate["applications"] if app["id"] == offer["application_id"])
            scorecards_data_for_offers[offer["application_id"]] = {
                "candidate_name": candidate["first_name"] + " " + candidate["last_name"],
                "candidate_data": {
                    "company": candidate["company"],
                    "title": candidate["title"],
                    "created_at": candidate["created_at"],
                    "recruiter_name": candidate["recruiter"]["name"] if "recruiter" in candidate and candidate["recruiter"] is not None else None,
                },
                "application_id": offer["application_id"],
                "application_data": {
                    "source": application["source"]["public_name"] if "source" in application and application["source"] is not None else None,
                    "credited_to": application["credited_to"]["name"] if "credited_to" in application and application["credited_to"] is not None else None,
                    "jobs": [job["name"] for job in application["jobs"]] if "jobs" in application and application["jobs"] is not None else [],
                    "prospective_department": application["prospective_department"],
                },
                "scorecards_data": [format_scorecard(scorecard) for scorecard in scorecards]
            }
        
        # Write the data to a JSON file
        with open("scorecards_data.json", "w") as f:
            json.dump(scorecards_data_for_offers, f)
        
