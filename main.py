import requests
from datetime import datetime
import os
from typing import List, Dict
import base64



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
    Fetches all scorecards for a given application id.
    
    Args:
        api_token: Greenhouse API token
        application_id: Application id
        
    Returns:
        List of scorecard dictionaries
    """
    print(f"Fetching scorecards for application {application_id}")
    headers = get_greenhouse_auth_headers(api_token)
    response = requests.get(
        f"{GREENHOUSE_BASE_URL}/applications/{application_id}/scorecards",
        headers=headers,
    )
    response.raise_for_status()
    scorecards = response.json()
    if not scorecards:
        return []
    return scorecards

def get_greenhouse_candidates(api_token: str, candidate_ids: List[int]) -> Dict:
    """
    Fetches all candidates for a given list of candidate ids.
    
    Args:
        api_token: Greenhouse API token
        candidate_ids: List of candidate ids
    """
    headers = get_greenhouse_auth_headers(api_token)
    page = 1
    candidates = []
    while True:
        print(f"Fetching candidates page {page}")
        response = requests.get(
            f"{GREENHOUSE_BASE_URL}/candidates",
            headers=headers,
            params={
                "page": page,
                "per_page": 500,
                "skip_count": True,
                "candidate_ids": ",".join(map(str, candidate_ids))
            }
        )
        response.raise_for_status()
        new_candidates = response.json()
        if not new_candidates:
            break
        candidates.extend(new_candidates)
        page += 1
    return candidates

if __name__ == "__main__":
    api_token = os.getenv("GREENHOUSE_API_TOKEN")
    accepted_offers = get_greenhouse_accepted_offers(api_token)
    print(f"Found {len(accepted_offers)} accepted offers")
    scorecard_data = {}
    candidate_data = {}
    i = 0
    for offer in accepted_offers:
        i += 1
        if i % 10 == 0:
            print(f"Processing offer {i} of {len(accepted_offers)}")
        scorecard_data[offer["candidate_id"]] = get_greenhouse_scorecards(api_token, offer["application_id"])
    candidate_ids = [offer["candidate_id"] for offer in accepted_offers]
    candidates = get_greenhouse_candidates(api_token, candidate_ids)
    for candidate in candidates:
        candidate_data[candidate["id"]] = candidate
    print(f"Found {len(scorecard_data)} scorecards and {len(candidate_data)} candidates")

