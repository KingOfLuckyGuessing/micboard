"""
Minimal Planning Center Services (PCO) integration
Kept only essential functions for OAuth, service types, teams, and positions
All scheduling logic has been moved to pco_scheduler.py
"""

import os
import json
import logging
import threading
import time
from typing import Optional, Dict, List, Any
import requests
from requests_oauthlib import OAuth2Session

import config
import pco_debug

# PCO Rate Limiting
PCO_RATE_LIMIT = 70
PCO_WINDOW_SECONDS = 20
PCO_REQUEST_DELAY = PCO_WINDOW_SECONDS / PCO_RATE_LIMIT

# Rate limiting state
_rate_limit_lock = threading.Lock()
_last_request_time = 0.0
_request_count = 0
_window_start_time = 0.0

PCO_API_BASE = 'https://api.planningcenteronline.com/services/v2'

def get_pco_credentials():
    """Get PCO credentials from environment or config."""
    client_id = os.environ.get('PCO_CLIENT_ID')
    client_secret = os.environ.get('PCO_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        try:
            oauth_creds = config.config_tree.get('oauth_credentials', {})
            client_id = oauth_creds.get('pco_client_id')
            client_secret = oauth_creds.get('pco_client_secret')
        except AttributeError:
            pass
    
    return client_id or '', client_secret or ''

def _rate_limit_request() -> None:
    """Enforce PCO rate limiting."""
    global _last_request_time, _request_count, _window_start_time
    
    with _rate_limit_lock:
        now = time.monotonic()
        
        if now - _window_start_time >= PCO_WINDOW_SECONDS:
            _request_count = 0
            _window_start_time = now
        
        if _request_count >= PCO_RATE_LIMIT:
            sleep_time = PCO_WINDOW_SECONDS - (now - _window_start_time)
            if sleep_time > 0:
                logging.warning(f"PCO rate limit reached, sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
                _request_count = 0
                _window_start_time = time.monotonic()
        
        time_since_last = now - _last_request_time
        if time_since_last < PCO_REQUEST_DELAY:
            time.sleep(PCO_REQUEST_DELAY - time_since_last)
        
        _last_request_time = time.monotonic()
        _request_count += 1

def _make_pco_request(session: requests.Session, url: str, params: Optional[Dict] = None, max_retries: int = 3) -> Optional[requests.Response]:
    """Make a rate-limited PCO API request with retry logic."""
    pco_debug.log_pco_request('GET', url, params, session.headers,
                              caller='planning_center_minimal._make_pco_request')
    for attempt in range(max_retries):
        try:
            _rate_limit_request()
            _started = time.monotonic()
            response = session.get(url, params=params, timeout=10)
            pco_debug.log_pco_response(response, caller='planning_center_minimal._make_pco_request',
                                       elapsed_ms=(time.monotonic() - _started) * 1000)
            
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = float(retry_after)
                    logging.warning(f"PCO rate limited, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                else:
                    wait_time = (2 ** attempt) * 5
                    logging.warning(f"PCO rate limited, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                continue
            
            if response.status_code == 404:
                logging.info(f"Resource not found (404): {url}")
                return response
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                logging.error(f"PCO API request timed out after {max_retries} attempts")
                return None
            else:
                wait_time = (2 ** attempt) * 2
                logging.warning(f"PCO API request timed out, retrying in {wait_time} seconds")
                time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logging.error(f"PCO API request failed after {max_retries} attempts: {e}")
                return None
            else:
                wait_time = (2 ** attempt) * 2
                logging.warning(f"PCO API request failed, retrying in {wait_time} seconds: {e}")
                time.sleep(wait_time)
    
    return None

def get_pco_session() -> Optional[requests.Session]:
    """Get an authenticated PCO session."""
    client_id, client_secret = get_pco_credentials()
    
    if not client_id or not client_secret:
        logging.warning("No PCO credentials available")
        return None
    
    session = requests.Session()
    session.auth = (client_id, client_secret)
    session.headers.update({
        'X-PCO-API-Version': '2023-08-01'
    })
    
    return session

def get_service_types() -> List[Dict[str, Any]]:
    """Fetch all service types from PCO."""
    session = get_pco_session()
    if not session:
        return []
    
    try:
        response = _make_pco_request(session, f"{PCO_API_BASE}/service_types")
        if not response:
            return []
        data = response.json()
        
        service_types = []
        for item in data.get('data', []):
            service_types.append({
                'id': item['id'],
                'name': item['attributes']['name'],
                'frequency': item['attributes']['frequency']
            })
        
        return service_types
    except Exception as e:
        logging.error(f"Error fetching PCO service types: {e}")
        return []

def get_teams(service_type_ids: List[str]) -> List[Dict[str, Any]]:
    """Fetch teams from specified service types, deduplicated by name."""
    session = get_pco_session()
    if not session:
        return []
    
    teams_by_name = {}
    
    for service_type_id in service_type_ids:
        try:
            url = f"{PCO_API_BASE}/service_types/{service_type_id}/teams"
            response = _make_pco_request(session, url)
            if not response:
                continue
            data = response.json()
            
            for item in data.get('data', []):
                team_name = item['attributes']['name']
                if team_name not in teams_by_name:
                    teams_by_name[team_name] = {
                        'id': item['id'],
                        'name': team_name,
                        'service_type_id': service_type_id
                    }
        except Exception as e:
            logging.error(f"Error fetching teams for service type {service_type_id}: {e}")
    
    return list(teams_by_name.values())

def get_positions(service_type_ids: List[str], team_name: str) -> List[Dict[str, Any]]:
    """Fetch positions for a specific team across service types, deduplicated by name."""
    session = get_pco_session()
    if not session:
        return []
    
    positions_by_name = {}
    
    for service_type_id in service_type_ids:
        try:
            # First, get all teams for this service type to find the team ID
            teams_url = f"{PCO_API_BASE}/service_types/{service_type_id}/teams"
            teams_response = _make_pco_request(session, teams_url)
            if not teams_response:
                continue
            teams_data = teams_response.json()
            
            # Find the team ID for the requested team name
            target_team_id = None
            for team_item in teams_data.get('data', []):
                if team_item['attributes']['name'] == team_name:
                    target_team_id = team_item['id']
                    break
            
            if not target_team_id:
                continue
            
            # Now get positions for this specific team
            positions_url = f"{PCO_API_BASE}/service_types/{service_type_id}/teams/{target_team_id}/team_positions"
            response = _make_pco_request(session, positions_url)
            if not response:
                continue
            positions_data = response.json()
            
            # Add positions for this team
            for item in positions_data.get('data', []):
                position_name = item['attributes']['name']
                
                if position_name not in positions_by_name:
                    positions_by_name[position_name] = {
                        'id': item['id'],
                        'name': position_name,
                        'team_id': target_team_id,
                        'service_type_id': service_type_id
                    }
                    
        except Exception as e:
            logging.error(f"Error fetching positions for team {team_name} in service type {service_type_id}: {e}")
    
    result = list(positions_by_name.values())
    logging.info(f"Returning {len(result)} unique positions for team '{team_name}'")
    return result
