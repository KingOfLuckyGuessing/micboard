"""Planning Center Services (PCO) integration for Micboard."""

import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any, Set
from urllib.parse import urlencode

import requests
from requests_oauthlib import OAuth2Session
from dateutil import parser as date_parser

import config
import pco_debug

# PCO Rate Limiting: 70 requests per 20-second window
PCO_RATE_LIMIT = 70
PCO_WINDOW_SECONDS = 20
PCO_REQUEST_DELAY = PCO_WINDOW_SECONDS / PCO_RATE_LIMIT  # ~0.286 seconds between requests

# Rate limiting state
_rate_limit_lock = threading.Lock()
_last_request_time = 0.0
_request_count = 0
_window_start_time = 0.0

# PCO API configuration
def get_pco_credentials():
    """Get PCO credentials from environment or config."""
    client_id = os.environ.get('PCO_CLIENT_ID')
    client_secret = os.environ.get('PCO_CLIENT_SECRET')
    
    # If not in environment, try config
    if not client_id or not client_secret:
        try:
            oauth_creds = config.config_tree.get('oauth_credentials', {})
            client_id = oauth_creds.get('pco_client_id')
            client_secret = oauth_creds.get('pco_client_secret')
        except AttributeError:
            # config_tree not loaded yet
            pass
    
    return client_id or '', client_secret or ''

PCO_API_BASE = 'https://api.planningcenteronline.com/services/v2'

# Global sync thread control
_sync_thread = None
_sync_stop_event = threading.Event()
_last_assignment_state = {}
_log_last: Dict[str, float] = {}

# Service type name cache - populated from the integrations page
_service_type_names_cache: Dict[str, str] = {}

# Simple in-memory cache for PCO schedules
_schedule_cache_lock = threading.Lock()
_schedule_cache: Dict[str, Any] = {
    'generated_at': None,
    'day_start_local': None,
    'window_end_local': None,
    'days': 1,
    'daily_schedules': {},  # service_type_id -> list
    'plan_of_day': {},  # service_type_id -> dict
    'plan_data': {},  # plan_id -> complete plan data including assignments
    'manual_plan_id': None  # manually selected plan ID
}

# Background thread control for schedule cache
_schedule_thread = None
_schedule_stop_event = threading.Event()

def _should_log_throttled(key: str, interval_seconds: float = 60.0) -> bool:
    now = time.monotonic()
    last = _log_last.get(key)
    if last is None or (now - last) >= interval_seconds:
        _log_last[key] = now
        return True
    return False

def throttled_info(key: str, message: str, interval_seconds: float = 60.0) -> None:
    if _should_log_throttled(key, interval_seconds):
        logging.info(message)

def log_api_request(method: str, url: str, params: dict = None, headers: dict = None) -> None:
    """Log PCO API request details."""
    log_data = {
        'method': method,
        'url': url,
        'params': params,
        'headers': {k: v for k, v in (headers or {}).items() if k.lower() not in ['authorization', 'x-pco-api-version']}
    }
    logging.info(f"PCO API REQUEST: {json.dumps(log_data, indent=2)}")

def log_api_response(status_code: int, response_data: dict = None, error: str = None) -> None:
    """Log PCO API response details."""
    if error:
        logging.error(f"PCO API RESPONSE ERROR: {status_code} - {error}")
    else:
        # Only log response summary, not full data to avoid spam
        if response_data and 'data' in response_data:
            data_count = len(response_data.get('data', []))
            logging.info(f"PCO API RESPONSE: {status_code} - {data_count} items returned")
        else:
            logging.info(f"PCO API RESPONSE: {status_code}")

def _rate_limit_request() -> None:
    """Enforce PCO rate limiting: 70 requests per 20-second window."""
    global _last_request_time, _request_count, _window_start_time
    
    with _rate_limit_lock:
        now = time.monotonic()
        
        # Reset window if needed
        if now - _window_start_time >= PCO_WINDOW_SECONDS:
            _request_count = 0
            _window_start_time = now
        
        # Check if we're at the limit
        if _request_count >= PCO_RATE_LIMIT:
            # Wait for the window to reset
            sleep_time = PCO_WINDOW_SECONDS - (now - _window_start_time)
            if sleep_time > 0:
                logging.warning(f"PCO rate limit reached, sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
                _request_count = 0
                _window_start_time = time.monotonic()
        
        # Enforce minimum delay between requests
        time_since_last = now - _last_request_time
        if time_since_last < PCO_REQUEST_DELAY:
            sleep_time = PCO_REQUEST_DELAY - time_since_last
            time.sleep(sleep_time)
        
        _last_request_time = time.monotonic()
        _request_count += 1

def _make_pco_request(session: requests.Session, url: str, params: Optional[Dict] = None, max_retries: int = 3) -> Optional[requests.Response]:
    """Make a rate-limited PCO API request with retry logic.

    All request/response logging goes through ``pco_debug`` (see that
    module for how to turn full-body logging on). The response is
    logged *before* ``raise_for_status()`` so an error body is captured
    even when the call then raises and is retried/returned as None.
    """
    import time as _time

    pco_debug.log_pco_request('GET', url, params, session.headers,
                              caller='planning_center._make_pco_request')

    for attempt in range(max_retries):
        try:
            _rate_limit_request()
            _started = _time.monotonic()
            response = session.get(url, params=params, timeout=10)
            _elapsed_ms = (_time.monotonic() - _started) * 1000
            pco_debug.log_pco_response(response, caller='planning_center._make_pco_request',
                                       elapsed_ms=_elapsed_ms)

            if response.status_code == 429:
                # Rate limited - check for Retry-After header
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = float(retry_after)
                    logging.warning(f"PCO rate limited, waiting {wait_time} seconds (Retry-After header)")
                    time.sleep(wait_time)
                else:
                    # Exponential backoff
                    wait_time = (2 ** attempt) * 5  # 5, 10, 20 seconds
                    logging.warning(f"PCO rate limited, waiting {wait_time} seconds (exponential backoff)")
                    time.sleep(wait_time)
                continue

            # Handle 404 specifically - it's often expected (e.g., no assignments yet)
            if response.status_code == 404:
                return response

            response.raise_for_status()

            return response

        except requests.exceptions.Timeout as e:
            if attempt == max_retries - 1:
                pco_debug.log_pco_response(None, caller='planning_center._make_pco_request',
                                           error=f"Timeout: {str(e)}")
                logging.error(f"PCO API request timed out after {max_retries} attempts: {e}")
                return None
            else:
                wait_time = (2 ** attempt) * 2  # 2, 4, 8 seconds
                logging.warning(f"PCO API request timed out, retrying in {wait_time} seconds: {e}")
                time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                pco_debug.log_pco_response(None, caller='planning_center._make_pco_request',
                                           error=str(e))
                logging.error(f"PCO API request failed after {max_retries} attempts: {e}")
                return None
            else:
                wait_time = (2 ** attempt) * 2  # 2, 4, 8 seconds
                logging.warning(f"PCO API request failed, retrying in {wait_time} seconds: {e}")
                time.sleep(wait_time)

    return None

def get_pco_session() -> Optional[requests.Session]:
    """Get an authenticated PCO session using Personal Access Token credentials."""
    client_id, client_secret = get_pco_credentials()
    
    if not client_id or not client_secret:
        logging.warning("No PCO credentials available")
        return None
    
    session = requests.Session()
    # Use Basic Authentication with Client ID as username and Client Secret as password
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
            # Use .get() throughout: one service type missing an
            # attribute (e.g. 'frequency' is absent for some types)
            # used to raise KeyError and discard the whole list,
            # returning [] even though PCO returned all the types.
            attributes = item.get('attributes') or {}
            service_types.append({
                'id': item.get('id'),
                'name': attributes.get('name') or f"Service {item.get('id')}",
                'frequency': attributes.get('frequency') or ''
            })

        return service_types
    except Exception as e:
        logging.error(f"Error fetching PCO service types: {e}")
        return []


def get_service_type_name(service_type_id: str) -> str:
    """Get friendly name for service type from cache or PCO API."""
    # Check cache first (populated from integrations page data)
    if service_type_id in _service_type_names_cache:
        return _service_type_names_cache[service_type_id]
    
    # Fallback to API call only if not in cache
    session = get_pco_session()
    if not session:
        return f"Service {service_type_id}"
    
    try:
        url = f"{PCO_API_BASE}/service_types/{service_type_id}"
        response = _make_pco_request(session, url)
        if not response:
            return f"Service {service_type_id}"
        
        data = response.json()
        service_type = data.get('data', {})
        if service_type and 'attributes' in service_type:
            name = service_type['attributes'].get('name', f"Service {service_type_id}")
            # Cache the result for future use
            _service_type_names_cache[service_type_id] = name
            return name
        
        return f"Service {service_type_id}"
    except Exception as e:
        logging.error(f"Error fetching service type name for {service_type_id}: {e}")
        return f"Service {service_type_id}"


def preload_service_type_names(service_type_ids: List[str]) -> None:
    """Preload service type names into cache to avoid individual API calls."""
    session = get_pco_session()
    if not session:
        return
    
    try:
        # Fetch all service types at once
        response = _make_pco_request(session, f"{PCO_API_BASE}/service_types")
        if not response:
            return
        
        data = response.json()
        for item in data.get('data', []):
            service_type_id = item['id']
            if service_type_id in service_type_ids:
                name = item['attributes'].get('name', f"Service {service_type_id}")
                _service_type_names_cache[service_type_id] = name
                logging.info(f"Cached service type name: {service_type_id} -> {name}")
    except Exception as e:
        logging.error(f"Error preloading service type names: {e}")


def populate_service_type_names_from_data(service_types_data: List[Dict[str, Any]]) -> None:
    """Populate service type names cache from service types data (e.g., from integrations page)."""
    for service_type in service_types_data:
        service_type_id = service_type.get('id')
        service_type_name = service_type.get('name')
        if service_type_id and service_type_name:
            _service_type_names_cache[service_type_id] = service_type_name
            logging.info(f"Cached service type name from data: {service_type_id} -> {service_type_name}")


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


def get_teams_for_service_type(service_type_id: str) -> List[Dict[str, Any]]:
    """Fetch teams for a specific service type with their service-type-specific IDs."""
    session = get_pco_session()
    if not session:
        return []
    
    try:
        url = f"{PCO_API_BASE}/service_types/{service_type_id}/teams"
        response = _make_pco_request(session, url)
        if not response:
            return []
        data = response.json()
        
        teams = []
        for item in data.get('data', []):
            teams.append({
                'id': item['id'],
                'name': item['attributes']['name'],
                'service_type_id': service_type_id
            })
        
        return teams
    except Exception as e:
        logging.error(f"Error fetching teams for service type {service_type_id}: {e}")
        return []


def get_positions(service_type_ids: List[str], team_name: str) -> List[Dict[str, Any]]:
    """Fetch positions for a specific team across service types, deduplicated by name."""
    session = get_pco_session()
    if not session:
        return []
    
    throttled_info(f"get_positions_{team_name}", f"Getting positions for team '{team_name}' across service types: {service_type_ids}")
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


def get_positions_for_team_in_service_type(service_type_id: str, team_id: str) -> List[Dict[str, Any]]:
    """Fetch positions for a specific team within a specific service type."""
    session = get_pco_session()
    if not session:
        return []
    
    try:
        positions_url = f"{PCO_API_BASE}/service_types/{service_type_id}/teams/{team_id}/team_positions"
        response = _make_pco_request(session, positions_url)
        if not response:
            return []
        positions_data = response.json()
        
        positions = []
        for item in positions_data.get('data', []):
            positions.append({
                'id': item['id'],
                'name': item['attributes']['name'],
                'team_id': team_id,
                'service_type_id': service_type_id
            })
        
        return positions
    except Exception as e:
        logging.error(f"Error fetching positions for team {team_id} in service type {service_type_id}: {e}")
        return []


def _fetch_complete_plan_data(service_type_id: str, plan_id: str) -> Optional[Dict[str, Any]]:
    """Fetch complete plan data including assignments and store in cache."""
    try:
        # Check if we already have this plan in cache
        with _schedule_cache_lock:
            if 'plan_data' in _schedule_cache and plan_id in _schedule_cache['plan_data']:
                logging.info(f"_fetch_complete_plan_data: Using cached data for plan {plan_id}")
                return _schedule_cache['plan_data'][plan_id]
        
        logging.info(f"_fetch_complete_plan_data: Fetching fresh data for plan {plan_id}")
        
        session = get_pco_session()
        if not session:
            return None
        
        # Get plan details
        url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}"
        response = _make_pco_request(session, url)
        if not response:
            return None
        
        plan_data = response.json()
        plan_info = plan_data.get('data', {})
        
        # Get plan times
        times_url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_times"
        times_response = _make_pco_request(session, times_url)
        service_times = []
        if times_response:
            times_data = times_response.json()
            service_times = times_data.get('data', [])
        
        # Get plan people (assignments) - handle 404 gracefully
        people_url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_people"
        assignments = []
        try:
            people_response = _make_pco_request(session, people_url)
            if people_response and people_response.status_code == 200:
                people_data = people_response.json()
                assignments = people_data.get('data', [])
            else:
                # 404 or other error - plan might not have assignments yet
                logging.info(f"_fetch_complete_plan_data: No assignments found for plan {plan_id} (this is normal for plans without assigned people)")
        except Exception as e:
            logging.info(f"_fetch_complete_plan_data: Could not fetch assignments for plan {plan_id}: {e}")
        
        # Build complete plan data
        complete_plan = {
            'plan_id': plan_id,
            'service_type_id': service_type_id,
            'plan_info': plan_info,
            'service_times': service_times,
            'assignments': assignments,
            'fetched_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Store in cache
        with _schedule_cache_lock:
            if 'plan_data' not in _schedule_cache:
                _schedule_cache['plan_data'] = {}
            _schedule_cache['plan_data'][plan_id] = complete_plan
        
        logging.info(f"_fetch_complete_plan_data: Stored complete data for plan {plan_id} with {len(assignments)} assignments")
        return complete_plan
        
    except Exception as e:
        logging.error(f"Error fetching complete plan data for {plan_id}: {e}")
        return None

def build_daily_schedule(service_type_id: str, lead_time_hours: int) -> List[Dict[str, Any]]:
    """Build schedule for today + next 7 days with live windows for a service type."""
    session = get_pco_session()
    if not session:
        return []
    
    try:
        # Get plans starting from today at midnight
        now_local = datetime.now(timezone.utc).astimezone()
        today_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        after_utc = today_midnight.astimezone(timezone.utc)
        
        # Get plans using future filter, limit to 2 plans
        url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans"
        params = {
            'filter': 'future',
            'order': 'sort_date',
            'per_page': 2  # Limit to 2 plans to reduce noise
        }
        
        response = _make_pco_request(session, url, params)
        if not response:
            return []
        data = response.json()
        
        plans = data.get('data', [])
        logging.info(f"Found {len(plans)} future plans for service type {service_type_id}")
        
        schedule = []
        
        for plan in plans:
            plan_id = plan['id']
            plan_title = plan['attributes']['title']
            plan_date = plan['attributes']['dates']
            
            # Fetch complete plan data including assignments and store in cache
            complete_plan_data = _fetch_complete_plan_data(service_type_id, plan_id)
            if not complete_plan_data:
                continue
            
            times_data = {'data': complete_plan_data.get('service_times', [])}
            
            if times_data.get('data'):
                service_times = []
                for time_item in times_data['data']:
                    time_str = time_item['attributes']['starts_at']
                    time_obj = date_parser.parse(time_str)
                    
                    # Convert from UTC to local time if needed
                    if time_obj.tzinfo is not None and time_obj.tzinfo.utcoffset(time_obj) is not None:
                        time_obj = time_obj.astimezone()
                    
                    time_name = time_item['attributes'].get('name', 'Service')
                    service_times.append({
                        'time_obj': time_obj,
                        'time_name': time_name
                    })
                
                service_times.sort(key=lambda x: x['time_obj'])
                
                # Calculate plan-wide live window: lead_time_hours before first service until end of day
                first_service_time = service_times[0]['time_obj']
                plan_live_start = first_service_time - timedelta(hours=lead_time_hours)
                plan_live_end = first_service_time.replace(hour=23, minute=59, second=59, microsecond=999999)
                
                # Create a single plan entry with all service times aggregated
                plan_entry = {
                    'plan_id': plan_id,
                    'plan_title': plan_title,
                    'dates': plan_date,
                    'service_type_id': service_type_id,
                    'live_start': plan_live_start.isoformat(),
                    'live_end': plan_live_end.isoformat(),
                    'lead_time_hours': lead_time_hours,
                    'services': []  # List of all service times for this plan
                }
                
                # Add all service times to the plan
                for service_time in service_times:
                    start_time = service_time['time_obj']
                    time_name = service_time['time_name']
                    
                    plan_entry['services'].append({
                        'service_name': time_name,
                        'service_start': start_time.isoformat()
                    })
                
                schedule.append(plan_entry)
        
        # Sort schedule by first service start time
        schedule.sort(key=lambda x: x['services'][0]['service_start'] if x['services'] else '')
        return schedule
        
    except Exception as e:
        logging.error(f"Error building daily schedule for service type {service_type_id}: {e}")
        return []





def build_multi_day_live_schedule(service_type_id: str, lead_time_hours: int, days: int = 8) -> List[Dict[str, Any]]:
    """Build a multi-day schedule (default 8-day rolling) showing when services should be live."""
    session = get_pco_session()
    if not session:
        return []

    try:
        now_local = datetime.now(timezone.utc).astimezone()
        today_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        after_utc = today_midnight.astimezone(timezone.utc)

        # Fetch plans using future filter
        url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans"
        params = {
            'filter': 'future',
            'order': 'sort_date',
            'per_page': 2  # Limit to 2 plans
        }
        response = _make_pco_request(session, url, params)
        if not response:
            return []
        data = response.json()

        plans = data.get('data', [])
        live_schedule: List[Dict[str, Any]] = []

        # Collect earliest per plan for later potential cross-plan boundaries if needed
        plan_earliest_local: List[Dict[str, Any]] = []

        # First pass: compute earliest per plan (local)
        for plan in plans:
            plan_id = plan['id']
            times_url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_times"
            times_response = _make_pco_request(session, times_url)
            if not times_response:
                continue
            times_data = times_response.json()
            if not times_data.get('data'):
                continue

            earliest_time = None
            for time_item in times_data['data']:
                time_obj = date_parser.parse(time_item['attributes']['starts_at'])
                if time_obj.tzinfo is not None and time_obj.tzinfo.utcoffset(time_obj) is not None:
                    time_obj = time_obj.astimezone()
                if earliest_time is None or time_obj < earliest_time:
                    earliest_time = time_obj
            if earliest_time is not None:
                plan_earliest_local.append({'plan_id': plan_id, 'earliest': earliest_time})

        # Sort earliest list by time
        plan_earliest_local.sort(key=lambda x: x['earliest'])
        plan_id_to_next_earliest: Dict[str, datetime] = {}
        for i, item in enumerate(plan_earliest_local):
            if i + 1 < len(plan_earliest_local):
                plan_id_to_next_earliest[item['plan_id']] = plan_earliest_local[i + 1]['earliest']

        # Second pass: build windows per plan times
        for plan in plans:
            plan_id = plan['id']
            plan_title = plan['attributes']['title']
            plan_date = plan['attributes']['dates']

            times_url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_times"
            times_response = _make_pco_request(session, times_url)
            if not times_response:
                continue
            times_data = times_response.json()
            if not times_data.get('data'):
                continue

            service_times: List[Dict[str, Any]] = []
            for time_item in times_data['data']:
                time_obj = date_parser.parse(time_item['attributes']['starts_at'])
                if time_obj.tzinfo is not None and time_obj.tzinfo.utcoffset(time_obj) is not None:
                    time_obj = time_obj.astimezone()
                service_times.append({
                    'time_obj': time_obj,
                    'time_name': time_item['attributes'].get('name', 'Service')
                })
            service_times.sort(key=lambda x: x['time_obj'])

            for i, service_time in enumerate(service_times):
                start_time = service_time['time_obj']
                time_name = service_time['time_name']

                # Live start/end
                live_start = start_time - timedelta(hours=lead_time_hours)
                if i + 1 < len(service_times):
                    next_service_start = service_times[i + 1]['time_obj']
                    live_end = next_service_start - timedelta(hours=lead_time_hours)
                else:
                    # If this is the last service in the plan, end at end of that day
                    live_end = start_time.replace(hour=23, minute=59, second=59, microsecond=999999)

                # Add to live schedule
                live_schedule.append({
                    'plan_id': plan_id,
                    'plan_title': plan_title,
                    'dates': plan_date,
                    'service_type_id': service_type_id,
                    'service_name': time_name,
                    'service_start': start_time.isoformat(),
                    'live_start': live_start.isoformat(),
                    'live_end': live_end.isoformat(),
                    'lead_time_hours': lead_time_hours
                })

        live_schedule.sort(key=lambda x: x['live_start'])
        return live_schedule

    except Exception as e:
        logging.error(f"Error building multi-day live schedule for service type {service_type_id}: {e}")
        return []

def _build_plan_of_day_from_cache(service_type_id: str, plan_id: str, lead_time_hours: int = 2) -> Optional[Dict[str, Any]]:
    """Build plan_of_day using cached assignment data - much faster than API calls."""
    try:
        # Get cached plan data
        with _schedule_cache_lock:
            plan_data = _schedule_cache.get('plan_data', {}).get(plan_id)
        
        if not plan_data:
            logging.warning(f"_build_plan_of_day_from_cache: No cached data for plan {plan_id}")
            return None
        
        assignments = plan_data.get('assignments', [])
        plan_info = plan_data.get('plan_info', {})
        service_times = plan_data.get('service_times', [])
        
        if not service_times:
            logging.warning(f"_build_plan_of_day_from_cache: No service times for plan {plan_id}")
            return None
        
        # Find earliest service time
        earliest_time = None
        for time_item in service_times:
            time_str = time_item['attributes']['starts_at']
            time_obj = date_parser.parse(time_str)
            if time_obj.tzinfo is not None and time_obj.tzinfo.utcoffset(time_obj) is not None:
                time_obj = time_obj.astimezone()
            
            if earliest_time is None or time_obj < earliest_time:
                earliest_time = time_obj
        
        if not earliest_time:
            logging.warning(f"_build_plan_of_day_from_cache: Could not parse service times for plan {plan_id}")
            return None
        
        # Compute live_time (start - lead_time)
        live_time = earliest_time - timedelta(hours=lead_time_hours)
        
        # Map names using cached assignment data
        names_by_slot: Dict[int, str] = {}
        pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
        
        # Check if we have a proper PCO configuration
        service_types = pco_config.get('service_types', [])
        config_found = False
        
        for st_conf in service_types:
            if st_conf['id'] != service_type_id:
                continue
            
            config_found = True
            # ID-based mappings from teams/positions
            for team in st_conf.get('teams', []):
                for pos in team.get('positions', []):
                    slot_number = pos.get('slot')
                    if not slot_number:
                        continue
                    
                    # Find assignment in cached data
                    person_name = _find_assignment_in_cache(assignments, {
                        'team_id': team.get('id'),
                        'position_id': pos.get('id')
                    })
                    if person_name:
                        names_by_slot[slot_number] = person_name
            
            # Reuse rules (name-based)
            for rule in st_conf.get('reuse_rules', []):
                slot_number = rule.get('slot')
                if not slot_number:
                    continue
                
                # Find assignment in cached data
                person_name = _find_assignment_in_cache(assignments, {
                    'team_name': rule.get('team_name'),
                    'position_name': rule.get('position_name')
                })
                if person_name:
                    names_by_slot[slot_number] = person_name
        
        # If no configuration found, use position name-based mapping
        if not config_found and assignments:
            logging.info(f"_build_plan_of_day_from_cache: No PCO config for service type {service_type_id}, using position name-based mapping")
            
            # Map positions by name (Mic 1 -> Slot 1, Mic 2 -> Slot 2, etc.)
            for assignment in assignments:
                attributes = assignment.get('attributes', {})
                person_name = attributes.get('name')
                position_name = attributes.get('team_position_name', '')
                
                if person_name and position_name:
                    # Check for "Mic N" pattern
                    if position_name.startswith('Mic '):
                        try:
                            # Extract the number from "Mic N"
                            mic_number = int(position_name.split(' ')[1])
                            if 1 <= mic_number <= 6:
                                names_by_slot[mic_number] = person_name
                                logging.info(f"Mapped {person_name} (position: {position_name}) to slot {mic_number}")
                        except (ValueError, IndexError):
                            pass
                    
                    # Also check for exact position name matches
                    if position_name == 'Mic 1':
                        names_by_slot[1] = person_name
                        logging.info(f"Mapped {person_name} to slot 1 (Mic 1)")
                    elif position_name == 'Mic 2':
                        names_by_slot[2] = person_name
                        logging.info(f"Mapped {person_name} to slot 2 (Mic 2)")
                    elif position_name == 'Mic 3':
                        names_by_slot[3] = person_name
                        logging.info(f"Mapped {person_name} to slot 3 (Mic 3)")
                    elif position_name == 'Mic 4':
                        names_by_slot[4] = person_name
                        logging.info(f"Mapped {person_name} to slot 4 (Mic 4)")
                    elif position_name == 'Mic 5':
                        names_by_slot[5] = person_name
                        logging.info(f"Mapped {person_name} to slot 5 (Mic 5)")
                    elif position_name == 'Mic 6':
                        names_by_slot[6] = person_name
                        logging.info(f"Mapped {person_name} to slot 6 (Mic 6)")
                    else:
                        pass
        
        result = {
            'plan_id': plan_id,
            'service_type_id': service_type_id,
            'service_type_name': get_service_type_name(service_type_id),
            'start_time': earliest_time.isoformat(),
            'live_time': live_time.isoformat(),
            'names_by_slot': names_by_slot,
            'plan_info': plan_info,
            'built_from_cache': True
        }
        
        logging.info(f"_build_plan_of_day_from_cache: Built plan_of_day for {plan_id} with {len(names_by_slot)} slot assignments")
        return result
        
    except Exception as e:
        logging.error(f"Error building plan_of_day from cache for {plan_id}: {e}")
        return None

def _find_assignment_in_cache(assignments: List[Dict], slot_mapping: Dict[str, Any]) -> Optional[str]:
    """Find assignment in cached assignment data.
    
    Since service types have different IDs for the same teams/positions,
    we always match by name rather than ID.
    """
    try:
        # Always use name-based matching since IDs differ across service types
        position_name = slot_mapping.get('position_name')
        
        # If we have an ID-based mapping, we still need to match by name
        # since the position name is what's consistent across service types
        if not position_name and 'position_id' in slot_mapping:
            # We would need to look up the position name from the ID,
            # but that would require API calls. For now, skip.
            return None
        
        if position_name:
            for assignment in assignments:
                attributes = assignment.get('attributes', {})
                team_position_name = attributes.get('team_position_name', '')
                
                # Check if the position name matches exactly
                if team_position_name == position_name:
                    # Return the person's name
                    person_name = attributes.get('name')
                    if person_name:
                        logging.info(f"Found assignment for position '{position_name}': {person_name}")
                        return person_name
                
                # Also check for "Mic N" pattern variations
                # (in case config uses "Mic 1" but PCO has "Mic 1" with different casing/spacing)
                if position_name.lower().replace(' ', '') == team_position_name.lower().replace(' ', ''):
                    person_name = attributes.get('name')
                    if person_name:
                        logging.info(f"Found assignment for position '{position_name}' (matched '{team_position_name}'): {person_name}")
                        return person_name
        
        return None
        
    except Exception as e:
        logging.error(f"Error finding assignment in cache: {e}")
        return None

def _find_assignment_in_cache_from_plan_id(plan_id: str, slot_mapping: Dict[str, Any]) -> Optional[str]:
    """Find assignment in cached data by plan ID."""
    try:
        # Get cached plan data
        with _schedule_cache_lock:
            plan_data = _schedule_cache.get('plan_data', {}).get(plan_id)
        
        if not plan_data:
            return None
        
        assignments = plan_data.get('assignments', [])
        return _find_assignment_in_cache(assignments, slot_mapping)
        
    except Exception as e:
        logging.error(f"Error finding assignment in cache for plan {plan_id}: {e}")
        return None

def _build_plan_of_day_for_service(service_type_id: str, lead_time_hours: int) -> Optional[Dict[str, Any]]:
    """Build today's plan-of-day for one service type with mapped names.
    Returns dict: { plan_id, service_type_id, start_time, live_time, title, names_by_slot } or None.
    """
    try:
        logging.info(f"_build_plan_of_day_for_service: Building plan of day for service type {service_type_id}")
        
        # Check if we have a manually selected plan
        manual_plan_id = get_manual_plan_selection()
        if manual_plan_id:
            logging.info(f"_build_plan_of_day_for_service: Using manually selected plan {manual_plan_id}")
            # Try to build from cache first (much faster)
            cached_result = _build_plan_of_day_from_cache(service_type_id, manual_plan_id, lead_time_hours)
            if cached_result:
                return cached_result
            # Fall back to API calls if cache miss
            logging.warning(f"_build_plan_of_day_for_service: Cache miss for manual plan {manual_plan_id}, falling back to API calls")
        
        # Use cached daily schedule and filter to today
        schedule = get_cached_daily_schedule(service_type_id)
        logging.info(f"_build_plan_of_day_for_service: Cached schedule for {service_type_id}: {len(schedule) if schedule else 0} items")
        
        if not schedule:
            logging.warning(f"_build_plan_of_day_for_service: No cached schedule found for service type {service_type_id}")
            return None
        
        # Check for manual plan selection
        manual_plan_id = get_manual_plan_selection()
        logging.info(f"_build_plan_of_day_for_service: Manual plan selection: {manual_plan_id}")
        
        # First, check if there's a scheduled service that should be live
        scheduled_live_plan = None
        now = datetime.now(timezone.utc).astimezone()
        
        for item in schedule:
            try:
                live_start = date_parser.parse(item['live_start']) if isinstance(item['live_start'], str) else item['live_start']
                live_end = date_parser.parse(item['live_end']) if isinstance(item['live_end'], str) else item['live_end']
                
                if live_start.tzinfo is not None and live_start.tzinfo.utcoffset(live_start) is not None:
                    live_start = live_start.astimezone()
                if live_end.tzinfo is not None and live_end.tzinfo.utcoffset(live_end) is not None:
                    live_end = live_end.astimezone()
                
                if live_start <= now <= live_end:
                    scheduled_live_plan = item
                    logging.info(f"_build_plan_of_day_for_service: Found scheduled live plan: {item['plan_id']}")
                    break
            except Exception as e:
                logging.warning(f"_build_plan_of_day_for_service: Error parsing schedule item: {e}")
                continue
        
        # Rule 1: If there's a scheduled service that should be live, use it (overrides manual selection)
        if scheduled_live_plan:
            logging.info(f"_build_plan_of_day_for_service: Using scheduled live plan (overrides manual selection)")
            selected_plan = scheduled_live_plan
        # Rule 2: If no scheduled service is live, check for manual selection
        elif manual_plan_id:
            # Find the manually selected plan in the schedule
            selected_plan = None
            for item in schedule:
                if item['plan_id'] == manual_plan_id:
                    selected_plan = item
                    logging.info(f"_build_plan_of_day_for_service: Using manually selected plan: {manual_plan_id}")
                    break
            
            if not selected_plan:
                logging.warning(f"_build_plan_of_day_for_service: Manual plan {manual_plan_id} not found in schedule")
                # Fall back to earliest plan for today
                selected_plan = None
        else:
            # No manual selection, use earliest plan for today
            selected_plan = None
        
        # If we have a selected plan (either scheduled live or manual), use it
        if selected_plan:
            # Try to build from cache first (much faster)
            cached_result = _build_plan_of_day_from_cache(service_type_id, selected_plan['plan_id'], lead_time_hours)
            if cached_result:
                logging.info(f"_build_plan_of_day_for_service: Using cached data for selected plan {selected_plan['plan_id']}")
                return cached_result
            
            # Fall back to building from schedule data if cache miss
            logging.warning(f"_build_plan_of_day_for_service: Cache miss for selected plan {selected_plan['plan_id']}, building from schedule data")
            
            # Find the earliest service time for the selected plan
            earliest = None
            if selected_plan.get('services'):
                for service in selected_plan['services']:
                    start = date_parser.parse(service['service_start']) if isinstance(service['service_start'], str) else service['service_start']
                    if start.tzinfo is not None and start.tzinfo.utcoffset(start) is not None:
                        start = start.astimezone()
                    
                    if earliest is None or start < earliest['start_time']:
                        earliest = {
                            'plan_id': selected_plan['plan_id'],
                            'title': selected_plan.get('plan_title'),
                            'start_time': start
                        }
            else:
                # Fallback if no services found
                earliest = {
                    'plan_id': selected_plan['plan_id'],
                    'title': selected_plan.get('plan_title'),
                    'start_time': date_parser.parse(selected_plan['start_time']) if isinstance(selected_plan['start_time'], str) else selected_plan['start_time']
                }
                if earliest['start_time'].tzinfo is not None and earliest['start_time'].tzinfo.utcoffset(earliest['start_time']) is not None:
                    earliest['start_time'] = earliest['start_time'].astimezone()
        else:
            # Choose earliest service time today (fallback)
            earliest = None
            now = datetime.now(timezone.utc).astimezone()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            for item in schedule:
                try:
                    # Check if this plan is for today
                    plan_date = date_parser.parse(item.get('dates', '')) if item.get('dates') else None
                    if plan_date:
                        if plan_date.tzinfo is not None and plan_date.tzinfo.utcoffset(plan_date) is not None:
                            plan_date = plan_date.astimezone()
                        plan_date = plan_date.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        if today_start <= plan_date <= today_end:
                            # This plan is for today, try cached data first
                            cached_result = _build_plan_of_day_from_cache(service_type_id, item['plan_id'], lead_time_hours)
                            if cached_result:
                                logging.info(f"_build_plan_of_day_for_service: Using cached data for today's plan {item['plan_id']}")
                                return cached_result
                            
                            # Fall back to building from schedule data
                            if item.get('services'):
                                for service in item['services']:
                                    start = date_parser.parse(service['service_start']) if isinstance(service['service_start'], str) else service['service_start']
                                    if start.tzinfo is not None and start.tzinfo.utcoffset(start) is not None:
                                        start = start.astimezone()
                                    
                                    if earliest is None or start < earliest['start_time']:
                                        earliest = {
                                            'plan_id': item['plan_id'],
                                            'title': item.get('plan_title'),
                                            'start_time': start
                                        }
                except Exception as e:
                    # Skip invalid plan items
                    continue
        
        if not earliest:
            logging.warning(f"_build_plan_of_day_for_service: No valid plan found for service type {service_type_id}")
            return None

        # Compute live_time (start - lead_time)
        live_time = earliest['start_time'] - timedelta(hours=lead_time_hours)

        # Map names using existing sync logic for configured slots
        names_by_slot: Dict[int, str] = {}
        pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
        service_types = pco_config.get('service_types', [])
        config_found = False
        
        for st_conf in service_types:
            if st_conf['id'] != service_type_id:
                continue
            
            config_found = True
            # ID-based mappings from teams/positions
            for team in st_conf.get('teams', []):
                for pos in team.get('positions', []):
                    slot_number = pos.get('slot')
                    if not slot_number:
                        continue
                    mapping = {
                        'service_type_id': service_type_id,
                        'team_id': team.get('id'),
                        'position_id': pos.get('id')
                    }
                    # Try cached assignment first, fall back to API call only if absolutely necessary
                    person_name = _find_assignment_in_cache_from_plan_id(earliest['plan_id'], mapping)
                    if not person_name:
                        # Don't make API call here - the data should have been fetched during build_daily_schedule
                        pass
                    if person_name:
                        names_by_slot[slot_number] = person_name
            # Reuse rules (name-based)
            for rule in st_conf.get('reuse_rules', []):
                slot_number = rule.get('slot')
                if not slot_number:
                    continue
                mapping = {
                    'team_name': rule.get('team_name'),
                    'position_name': rule.get('position_name')
                }
                # Try cached assignment first, fall back to API call only if absolutely necessary
                person_name = _find_assignment_in_cache_from_plan_id(earliest['plan_id'], mapping)
                if not person_name:
                    # Don't make API call here - the data should have been fetched during build_daily_schedule
                    pass
                if person_name:
                    names_by_slot[slot_number] = person_name
        
        # If no configuration found, use position name-based mapping
        if not config_found:
            logging.info(f"_build_plan_of_day_for_service: No PCO config for service type {service_type_id}, using position name-based mapping")
            # Get cached plan data
            with _schedule_cache_lock:
                plan_data = _schedule_cache.get('plan_data', {}).get(earliest['plan_id'])
            
            if plan_data:
                assignments = plan_data.get('assignments', [])
                
                # Map positions by name (Mic 1 -> Slot 1, Mic 2 -> Slot 2, etc.)
                for assignment in assignments:
                    attributes = assignment.get('attributes', {})
                    person_name = attributes.get('name')
                    position_name = attributes.get('team_position_name', '')
                    
                    if person_name and position_name:
                        # Check for "Mic N" pattern
                        if position_name.startswith('Mic '):
                            try:
                                # Extract the number from "Mic N"
                                mic_number = int(position_name.split(' ')[1])
                                if 1 <= mic_number <= 6:
                                    names_by_slot[mic_number] = person_name
                                    logging.info(f"Mapped {person_name} (position: {position_name}) to slot {mic_number}")
                            except (ValueError, IndexError):
                                pass
                        
                        # Also check for exact position name matches
                        if position_name == 'Mic 1':
                            names_by_slot[1] = person_name
                            logging.info(f"Mapped {person_name} to slot 1 (Mic 1)")
                        elif position_name == 'Mic 2':
                            names_by_slot[2] = person_name
                            logging.info(f"Mapped {person_name} to slot 2 (Mic 2)")
                        elif position_name == 'Mic 3':
                            names_by_slot[3] = person_name
                            logging.info(f"Mapped {person_name} to slot 3 (Mic 3)")
                        elif position_name == 'Mic 4':
                            names_by_slot[4] = person_name
                            logging.info(f"Mapped {person_name} to slot 4 (Mic 4)")
                        elif position_name == 'Mic 5':
                            names_by_slot[5] = person_name
                            logging.info(f"Mapped {person_name} to slot 5 (Mic 5)")
                        elif position_name == 'Mic 6':
                            names_by_slot[6] = person_name
                            logging.info(f"Mapped {person_name} to slot 6 (Mic 6)")
                        else:
                            pass

        result = {
            'plan_id': earliest['plan_id'],
            'service_type_id': service_type_id,
            'service_type_name': get_service_type_name(service_type_id),
            'title': earliest.get('title'),
            'start_time': earliest['start_time'].isoformat(),
            'live_time': live_time.isoformat(),
            'names_by_slot': names_by_slot
        }
        logging.info(f"_build_plan_of_day_for_service: Built plan of day for {service_type_id}: {result}")
        logging.info(f"_build_plan_of_day_for_service: names_by_slot has {len(names_by_slot)} entries: {names_by_slot}")
        return result
    except Exception as e:
        logging.error(f"Error building plan-of-day for {service_type_id}: {e}")
        return None


def _refresh_schedule_cache(days: int = 8) -> None:
    """Refresh the in-memory schedule cache for all configured service types."""
    logging.info(f"_refresh_schedule_cache: Starting schedule cache refresh for {days} days")
    
    pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
    service_types = pco_config.get('service_types', [])
    lead_time_hours = pco_config.get('lead_time_hours', 2)
    
    logging.info(f"_refresh_schedule_cache: Found {len(service_types)} service types, lead_time_hours={lead_time_hours}")
    
    if not service_types:
        logging.warning("_refresh_schedule_cache: No service types configured")
        return

    now_local = datetime.now(timezone.utc).astimezone()
    today_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    daily_schedules: Dict[str, Any] = {}
    plan_of_day: Dict[str, Any] = {}

    for st in service_types:
        st_id = st['id']
        try:
            # Build daily schedule with live windows (using simple after filter)
            daily_schedules[st_id] = build_daily_schedule(st_id, lead_time_hours)
            # Only build plan_of_day if we have a daily schedule
            if daily_schedules[st_id]:
                pod = _build_plan_of_day_for_service(st_id, lead_time_hours)
                if pod:
                    plan_of_day[st_id] = pod
        except Exception as e:
            logging.error(f"Schedule cache refresh error for service type {st_id}: {e}")

    with _schedule_cache_lock:
        _schedule_cache['generated_at'] = datetime.now(timezone.utc)
        _schedule_cache['day_start_local'] = today_midnight
        _schedule_cache['window_end_local'] = today_midnight  # Not used with simple after filter
        _schedule_cache['days'] = days  # Use the actual days parameter
        _schedule_cache['daily_schedules'] = daily_schedules
        _schedule_cache['plan_of_day'] = plan_of_day

    logging.info(f"_refresh_schedule_cache: Schedule cache refreshed with future filter. plan_of_day: {plan_of_day}")


def get_cached_daily_schedule(service_type_id: str) -> List[Dict[str, Any]]:
    """Get cached daily schedule for a service type."""
    with _schedule_cache_lock:
        cached = _schedule_cache.get('daily_schedules', {}).get(service_type_id, [])
        if not cached:
            # No cached schedule available
            pass
        return cached


def get_cached_plan_of_day() -> Dict[str, Any]:
    with _schedule_cache_lock:
        return _schedule_cache.get('plan_of_day', {})

def get_manual_plan_selection() -> Optional[str]:
    """Get the currently manually selected plan ID, if any."""
    with _schedule_cache_lock:
        return _schedule_cache.get('manual_plan_id')

def set_manual_plan_selection(plan_id: Optional[str]) -> None:
    """Set a manually selected plan ID."""
    with _schedule_cache_lock:
        if plan_id:
            _schedule_cache['manual_plan_id'] = plan_id
            logging.info(f"Manual plan selection set to: {plan_id}")
        else:
            _schedule_cache.pop('manual_plan_id', None)
            logging.info("Manual plan selection cleared")

def _update_plan_of_day_in_cache(service_type_id: str, plan_of_day: Dict[str, Any]) -> None:
    """Update the plan_of_day for a specific service type in the cache."""
    with _schedule_cache_lock:
        if 'plan_of_day' not in _schedule_cache:
            _schedule_cache['plan_of_day'] = {}
        _schedule_cache['plan_of_day'][service_type_id] = plan_of_day
        logging.info(f"Updated plan_of_day for service type {service_type_id}")
    
    # Now update the actual slots with the names from plan_of_day
    if plan_of_day and 'names_by_slot' in plan_of_day:
        _update_slots_with_names(plan_of_day['names_by_slot'])

def _update_slots_with_names(names_by_slot: Dict[int, str]) -> None:
    """Update slot extended_names based on the names_by_slot mapping."""
    logging.info(f"_update_slots_with_names: Updating {len(names_by_slot)} slots with names")
    
    for slot_number, person_name in names_by_slot.items():
        try:
            update_data = {
                'slot': slot_number,
                'extended_name': person_name
            }
            config.update_slot(update_data)
            logging.info(f"Updated slot {slot_number} with name: {person_name}")
        except Exception as e:
            logging.error(f"Error updating slot {slot_number}: {e}")
    
    # Also clear names for slots not in the mapping
    for slot in config.config_tree.get('slots', []):
        slot_number = slot['slot']
        if slot_number not in names_by_slot and slot.get('extended_name'):
            try:
                update_data = {
                    'slot': slot_number,
                    'extended_name': ''
                }
                config.update_slot(update_data)
                logging.info(f"Cleared name for slot {slot_number}")
            except Exception as e:
                logging.error(f"Error clearing slot {slot_number}: {e}")

def force_refresh_schedule_cache() -> None:
    """Force a refresh of the schedule cache and restart the background thread."""
    logging.info("🔄 force_refresh_schedule_cache: Forcing schedule cache refresh")
    stop_schedule_cache_thread()
    refresh_schedule_cache_now(days=8)
    start_schedule_cache_thread(days=8)
    logging.info("✅ force_refresh_schedule_cache: Schedule cache refresh completed")


def refresh_schedule_cache_now(days: int = 8) -> None:
    """Public method to force a synchronous schedule cache refresh."""
    logging.info(f"Refreshing schedule cache for {days} days")
    _refresh_schedule_cache(days)


def schedule_cache_worker(days: int = 8) -> None:
    logging.info(f"Schedule cache thread started with {days} days window")
    while not _schedule_stop_event.is_set():
        try:
            _refresh_schedule_cache(days)
        except Exception as e:
            logging.error(f"Schedule cache worker error: {e}")

        # Sleep until next midnight local or 5 minutes if we're in a live service window
        try:
            now = datetime.now(timezone.utc).astimezone()
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
            
            # Check if we're currently in a live service window
            is_live = False
            try:
                pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
                service_types = pco_config.get('service_types', [])
                lead_time_hours = pco_config.get('lead_time_hours', 2)
                
                for st in service_types:
                    st_id = st['id']
                    live_plan = find_live_plan(st_id, lead_time_hours)
                    if live_plan:
                        is_live = True
                        break
            except Exception:
                pass
            
            if is_live:
                # Refresh every 5 minutes during live service
                sleep_seconds = 300.0
            else:
                # Refresh at midnight or every 30 minutes
                sleep_seconds = max(60.0, min(1800.0, (tomorrow - now).total_seconds()))
        except Exception:
            sleep_seconds = 1800.0

        # Sleep in small chunks to allow quick stop
        slept = 0.0
        while slept < sleep_seconds and not _schedule_stop_event.is_set():
            time.sleep(min(60.0, sleep_seconds - slept))
            slept += min(60.0, sleep_seconds - slept)

    logging.info("Schedule cache thread stopped")


def start_schedule_cache_thread(days: int = 8) -> None:
    global _schedule_thread
    if _schedule_thread and _schedule_thread.is_alive():
        logging.warning("Schedule cache thread already running")
        return
    
    # Clear the cache on startup to avoid stale data
    with _schedule_cache_lock:
        _schedule_cache['plan_data'] = {}
        logging.info("Cleared plan_data cache on startup")
    
    _schedule_stop_event.clear()
    _schedule_thread = threading.Thread(target=schedule_cache_worker, kwargs={'days': days}, daemon=True)
    _schedule_thread.start()
    logging.info(f"Schedule cache thread started with {days} days window")


def stop_schedule_cache_thread() -> None:
    global _schedule_thread
    if not _schedule_thread:
        return
    _schedule_stop_event.set()
    _schedule_thread.join(timeout=5)
    _schedule_thread = None


def find_live_plan(service_type_id: str, lead_time_hours: int) -> Optional[Dict[str, Any]]:
    """Find the currently live plan for a service type."""
    session = get_pco_session()
    if not session:
        return None
    
    try:
        # First try to use cached daily schedule to avoid network calls
        cached = get_cached_daily_schedule(service_type_id)
        if cached:
            now = datetime.now(timezone.utc).astimezone()
            # First, check for any entry whose live window contains now
            try:
                live_now = []
                for item in cached:
                    live_start = date_parser.parse(item['live_start']) if isinstance(item['live_start'], str) else item['live_start']
                    live_end = date_parser.parse(item['live_end']) if isinstance(item['live_end'], str) else item['live_end']
                    if live_start.tzinfo is not None and live_start.tzinfo.utcoffset(live_start) is not None:
                        live_start = live_start.astimezone()
                    if live_end.tzinfo is not None and live_end.tzinfo.utcoffset(live_end) is not None:
                        live_end = live_end.astimezone()
                    if live_start <= now <= live_end:
                        live_now.append({
                            'plan_id': item['plan_id'],
                            'plan_title': item.get('plan_title'),
                            'dates': item.get('dates'),
                            'service_start': date_parser.parse(item['service_start']) if isinstance(item['service_start'], str) else item['service_start'],
                            'live_end': live_end
                        })
                if live_now:
                    # Choose the one that started earliest
                    live_now.sort(key=lambda x: x['service_start'])
                    current = live_now[0]
                    return {
                        'id': current['plan_id'],
                        'title': current['plan_title'],
                        'dates': current['dates'],
                        'service_type_id': service_type_id,
                        'start_time': current['service_start'],
                        'end_time': current['live_end']
                    }
            except Exception:
                pass

            # Otherwise, return the next upcoming window by live_start
            upcoming = []
            for item in cached:
                try:
                    live_start = date_parser.parse(item['live_start']) if isinstance(item['live_start'], str) else item['live_start']
                    if live_start.tzinfo is not None and live_start.tzinfo.utcoffset(live_start) is not None:
                        live_start = live_start.astimezone()
                    if live_start > now:
                        upcoming.append((live_start, item))
                except Exception:
                    continue
            if upcoming:
                upcoming.sort(key=lambda t: t[0])
                _, item = upcoming[0]
                start_time = date_parser.parse(item['service_start']) if isinstance(item['service_start'], str) else item['service_start']
                end_time = date_parser.parse(item['live_end']) if isinstance(item['live_end'], str) else item['live_end']
                if end_time.tzinfo is not None and end_time.tzinfo.utcoffset(end_time) is not None:
                    end_time = end_time.astimezone()
                return {
                    'id': item['plan_id'],
                    'title': item.get('plan_title'),
                    'dates': item.get('dates'),
                    'service_type_id': service_type_id,
                    'start_time': start_time,
                    'end_time': end_time
                }

        # Fallback to network path if cache not available
        # Use future filter to get upcoming plans
        url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans"
        params = {
            'filter': 'future',
            'order': 'sort_date',
            'per_page': 2  # Limit to 2 plans
        }
        response = _make_pco_request(session, url, params)
        if not response:
            return []
        data = response.json()
        
        plans = data.get('data', [])
        
        # Get current time in UTC
        now_utc = datetime.now(timezone.utc)
        
        # Use UTC time for comparison since PCO times are in UTC
        now = now_utc
        
        # Build a schedule using the earliest time per plan (earliest time defines the plan's live window)
        schedule = []
        for plan in plans:
            plan_id = plan['id']
            plan_title = plan['attributes']['title']

            # Get service times for this plan
            times_url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_times"
            times_response = _make_pco_request(session, times_url)
            if not times_response:
                continue
            times_data = times_response.json()

            if times_data.get('data'):
                earliest_time = None
                for time_item in times_data['data']:
                    time_str = time_item['attributes']['starts_at']
                    time_obj = date_parser.parse(time_str)

                    # Convert from UTC to local time if needed
                    if time_obj.tzinfo is not None and time_obj.tzinfo.utcoffset(time_obj) is not None:
                        time_obj = time_obj.astimezone()

                    if earliest_time is None or time_obj < earliest_time:
                        earliest_time = time_obj

                if earliest_time is not None:
                    schedule.append({
                        'plan_id': plan_id,
                        'plan_title': plan_title,
                        'dates': plan['attributes']['dates'],
                        'service_type_id': service_type_id,
                        'start_time': earliest_time
                    })
            else:
                logging.warning(f"No service times found for plan {plan_id}")
        
        # Sort schedule by start time
        schedule.sort(key=lambda x: x['start_time'])
        
        # Try to find a service that is currently within its live window
        current_plan = None
        for i, service in enumerate(schedule):
            start_time = service['start_time']
            live_start = start_time - timedelta(hours=lead_time_hours)
            # Next plan's earliest start (if any)
            next_start = schedule[i + 1]['start_time'] if (i + 1) < len(schedule) else None
            end_of_day = start_time.replace(hour=23, minute=59, second=59, microsecond=999999)
            if next_start:
                candidate_end = next_start - timedelta(hours=lead_time_hours)
                live_end = candidate_end if candidate_end < end_of_day else end_of_day
            else:
                live_end = end_of_day
            if live_start <= now <= live_end:
                current_plan = {
                    'id': service['plan_id'],
                    'title': service['plan_title'],
                    'dates': service['dates'],
                    'service_type_id': service_type_id,
                    'start_time': start_time,
                    'end_time': live_end
                }
                break
        if current_plan:
            logging.info(f"Found live plan: {current_plan['id']} - {current_plan['title']}")
            return current_plan
        
        # Find the next upcoming plan (by earliest start)
        next_service = None
        for service in schedule:
            if service['start_time'] > now:
                next_service = service
                break
        
        if not next_service:
            logging.info(f"No upcoming services found for service type {service_type_id}")
            return None
        
        # Calculate live window for the next plan
        lead_time_start = next_service['start_time'] - timedelta(hours=lead_time_hours)
        # Determine live_end as earlier of next plan's live start or end of day
        next_service_time = None
        for service in schedule:
            if service['start_time'] > next_service['start_time']:
                next_service_time = service['start_time']
                break
        end_of_day = next_service['start_time'].replace(hour=23, minute=59, second=59, microsecond=999999)
        if next_service_time:
            candidate_end = next_service_time - timedelta(hours=lead_time_hours)
            live_end = candidate_end if candidate_end < end_of_day else end_of_day
        else:
            live_end = end_of_day
        
        throttled_info(
            f"live_summary_{service_type_id}",
            f"Next service: {next_service['plan_title']} at {next_service['start_time']} | window {lead_time_start} -> {live_end} | is_live={lead_time_start <= now <= live_end}",
            interval_seconds=60.0
        )
        
        if service_type_id == "769651":
            throttled_info(
                f"ngr_summary_{service_type_id}",
                f"NORTH GEORGIA REVIVAL - Next service: {next_service['plan_title']} at {next_service['start_time']} | window {lead_time_start} -> {live_end} | now={now} | is_live={lead_time_start <= now <= live_end}",
                interval_seconds=60.0
            )
        
        if lead_time_start <= now <= live_end:
            logging.info(f"Found live plan: {next_service['plan_id']} - {next_service['plan_title']}")
            return {
                'id': next_service['plan_id'],
                'title': next_service['plan_title'],
                'dates': next_service['dates'],
                'service_type_id': service_type_id,
                'start_time': next_service['start_time'],
                'end_time': live_end
            }
        
        return None
        
    except Exception as e:
        logging.error(f"Error finding live plan for service type {service_type_id}: {e}")
        return None


def _find_assignment(plan_id: str, service_type_id: str, slot_mapping: Dict[str, Any]) -> Optional[str]:
    """Find a person assignment for a slot based on its mapping configuration."""
    try:
        
        session = get_pco_session()
        if not session:
            logging.warning(f"_find_assignment: No PCO session available for plan {plan_id}")
            return None
        
        # Check if this is an ID-based mapping (teams/positions)
        if 'team_id' in slot_mapping and 'position_id' in slot_mapping:
            logging.info(f"_find_assignment: Looking for ID-based assignment - team_id: {slot_mapping['team_id']}, position_id: {slot_mapping['position_id']}")
            
            # Get assignments for this plan
            url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_people"
            response = _make_pco_request(session, url)
            if not response:
                logging.warning(f"_find_assignment: No response for plan_people API call for plan {plan_id}")
                return None
            
            data = response.json()
            assignments = data.get('data', [])
            logging.info(f"_find_assignment: Found {len(assignments)} assignments for plan {plan_id}")
            
            # Find assignment for this team/position
            for assignment in assignments:
                person = assignment.get('person', {})
                team_position = assignment.get('team_position', {})
                team = team_position.get('team', {})
                position = team_position.get('position', {})
                
                
                if (team.get('id') == slot_mapping['team_id'] and
                    position.get('id') == slot_mapping['position_id']):
                    person_name = person.get('name')
                    logging.info(f"_find_assignment: Found matching assignment: {person_name}")
                    return person_name
        
        # Check if this is a name-based mapping (reuse rules)
        elif 'team_name' in slot_mapping and 'position_name' in slot_mapping:
            logging.info(f"_find_assignment: Looking for name-based assignment - team_name: {slot_mapping['team_name']}, position_name: {slot_mapping['position_name']}")
            
            # Try team_members endpoint first (includes team and position data)
            url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/team_members"
            params = {'include': 'person,team_position'}
            response = _make_pco_request(session, url, params)
            
            if response:
                data = response.json()
                assignments = data.get('data', [])
                included = {}
                for item in data.get('included', []):
                    key = f"{item['type']}-{item['id']}"
                    included[key] = item
                
                logging.info(f"_find_assignment: Found {len(assignments)} team members for plan {plan_id}")
                
                # Find assignment for this team/position by name
                for assignment in assignments:
                    # Get person name
                    person_ref = assignment.get('relationships', {}).get('person', {}).get('data', {})
                    if not person_ref or 'type' not in person_ref or 'id' not in person_ref:
                        continue
                    person_key = f"{person_ref['type']}-{person_ref['id']}"
                    person_data = included.get(person_key, {})
                    person_name = person_data.get('attributes', {}).get('name', '')
                    
                    # Get position name
                    position_ref = assignment.get('relationships', {}).get('team_position', {}).get('data', {})
                    if not position_ref or 'type' not in position_ref or 'id' not in position_ref:
                        continue
                    position_key = f"{position_ref['type']}-{position_ref['id']}"
                    position_data = included.get(position_key, {})
                    position_name = position_data.get('attributes', {}).get('name', '')
                    
                    # Get team name
                    team_ref = assignment.get('relationships', {}).get('team', {}).get('data', {})
                    if not team_ref or 'type' not in team_ref or 'id' not in team_ref:
                        continue
                    team_key = f"{team_ref['type']}-{team_ref['id']}"
                    team_data = included.get(team_key, {})
                    team_name = team_data.get('attributes', {}).get('name', '')
                    
                    # Normalize case for comparison
                    normalized_team_name = team_name.lower().strip() if team_name else ""
                    normalized_position_name = position_name.lower().strip() if position_name else ""
                    normalized_mapping_team = slot_mapping['team_name'].lower().strip() if slot_mapping.get('team_name') else ""
                    normalized_mapping_position = slot_mapping['position_name'].lower().strip() if slot_mapping.get('position_name') else ""
                    
                    if (normalized_team_name == normalized_mapping_team and 
                        normalized_position_name == normalized_mapping_position):
                        logging.info(f"_find_assignment: Found matching assignment: {person_name} for {team_name} - {position_name}")
                        return person_name
            else:
                # Fallback to plan_people endpoint
                logging.info(f"_find_assignment: team_members failed, trying plan_people...")
                url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_people"
                response = _make_pco_request(session, url)
                if not response:
                    logging.warning(f"_find_assignment: No response for plan_people API call for plan {plan_id}")
                    return None
                
                data = response.json()
                assignments = data.get('data', [])
                logging.info(f"_find_assignment: Found {len(assignments)} assignments for plan {plan_id}")
                
                # Find assignment for this team/position by name
                for assignment in assignments:
                    # The plan_people endpoint returns data in attributes
                    attributes = assignment.get('attributes', {})
                    person_name = attributes.get('name', '')
                    position_name = attributes.get('team_position_name', '')
                    
                    
                    # For plan_people endpoint, we need to get team name from relationships
                    relationships = assignment.get('relationships', {})
                    team_ref = relationships.get('team', {}).get('data', {})
                    team_id = team_ref.get('id', '')
                    
                    # Normalize case for comparison
                    normalized_position_name = position_name.lower().strip() if position_name else ""
                    normalized_mapping_position = slot_mapping['position_name'].lower().strip() if slot_mapping.get('position_name') else ""
                    normalized_mapping_team = slot_mapping['team_name'].lower().strip() if slot_mapping.get('team_name') else ""
                    
                    # For plan_people endpoint, we need to get team name from relationships
                    # For now, let's try to match by position name only since we know the team is "Band"
                    if (normalized_position_name == normalized_mapping_position and 
                        normalized_mapping_team == 'band'):
                        logging.info(f"_find_assignment: Found matching assignment: {person_name} for position {position_name}")
                        return person_name
        
        logging.warning(f"_find_assignment: No matching assignment found for plan {plan_id} with mapping {slot_mapping}")
        return None
        
    except Exception as e:
        logging.error(f"Error finding assignment for plan {plan_id}: {e}")
        return None


def sync_assignments():
    """Main sync function to update slot assignments from PCO."""
    global _last_assignment_state
    
    logging.info("=== Starting PCO sync ===")
    
    client_id, client_secret = get_pco_credentials()
    if not client_id or not client_secret:
        logging.warning("PCO sync skipped - no credentials")
        return
    
    # Get PCO configuration from config tree
    pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
    if not pco_config:
        logging.warning("PCO sync skipped - no configuration")
        return
    
    lead_time_hours = pco_config.get('lead_time_hours', 2)
    service_types_config = pco_config.get('service_types', [])
    manual_plan_id = pco_config.get('manual_plan_id')
    
    # Ensure schedule cache is populated
    for st in service_types_config:
        st_id = st['id']
        cached_schedule = get_cached_daily_schedule(st_id)
        if not cached_schedule:
            logging.info(f"No cached schedule for service type {st_id}, refreshing cache")
            refresh_schedule_cache_now(days=8)
            break
    
    current_assignments = {}
    
    for service_type_config in service_types_config:
        service_type_id = service_type_config['id']
        
        # Determine which plan to use
        plan = None
        if manual_plan_id:
            plan = {'id': manual_plan_id, 'service_type_id': service_type_id}
            logging.info(f"Using manual plan ID: {manual_plan_id}")
        else:
            # Use cached daily schedule to find live plan instead of making API calls
            cached_schedule = get_cached_daily_schedule(service_type_id)
            if cached_schedule:
                now = datetime.now(timezone.utc).astimezone()
                for item in cached_schedule:
                    live_start = date_parser.parse(item['live_start']) if isinstance(item['live_start'], str) else item['live_start']
                    live_end = date_parser.parse(item['live_end']) if isinstance(item['live_end'], str) else item['live_end']
                    if live_start.tzinfo is not None and live_start.tzinfo.utcoffset(live_start) is not None:
                        live_start = live_start.astimezone()
                    if live_end.tzinfo is not None and live_end.tzinfo.utcoffset(live_end) is not None:
                        live_end = live_end.astimezone()
                    
                    if live_start <= now <= live_end:
                        plan = {'id': item['plan_id'], 'service_type_id': service_type_id}
                        logging.info(f"Found live plan {plan['id']} from cache for service type {service_type_id}")
                        break
                
                if not plan:
                    # No live plan found, use the first upcoming plan
                    for item in cached_schedule:
                        live_start = date_parser.parse(item['live_start']) if isinstance(item['live_start'], str) else item['live_start']
                        if live_start.tzinfo is not None and live_start.tzinfo.utcoffset(live_start) is not None:
                            live_start = live_start.astimezone()
                        if live_start > now:
                            plan = {'id': item['plan_id'], 'service_type_id': service_type_id}
                            logging.info(f"Using next upcoming plan {plan['id']} from cache for service type {service_type_id}")
                            break
            else:
                logging.warning(f"No cached schedule available for service type {service_type_id}")
                # Try to find live plan directly if no cache
                plan = find_live_plan(service_type_id, lead_time_hours)
                if plan:
                    plan = {'id': plan['id'], 'service_type_id': service_type_id}
        
        if not plan:
            logging.warning(f"No live plan found for service type {service_type_id}")
            continue
        
        logging.info(f"Using plan {plan['id']} for service type {service_type_id}")
        
        # Process team mappings
        for team_config in service_type_config.get('teams', []):
            for position_config in team_config.get('positions', []):
                slot_number = position_config.get('slot')
                if not slot_number:
                    continue
                
                # Build mapping info
                mapping = {
                    'service_type_id': service_type_id,
                    'team_id': team_config.get('id'),
                    'position_id': position_config.get('id')
                }
                
                person_name = _find_assignment(plan['id'], service_type_id, mapping)
                if person_name:
                    current_assignments[slot_number] = person_name
        
        # Process reuse rules
        reuse_rules = service_type_config.get('reuse_rules', [])
        
        for rule in reuse_rules:
            slot_number = rule.get('slot')
            if not slot_number:
                logging.warning(f"Rule missing slot number: {rule}")
                continue
            
            # Build mapping info
            mapping = {
                'team_name': rule.get('team_name'),
                'position_name': rule.get('position_name')
            }
            
            person_name = _find_assignment(plan['id'], service_type_id, mapping)
            if person_name:
                current_assignments[slot_number] = person_name
                logging.info(f"Found assignment for slot {slot_number}: {person_name}")
            else:
                logging.warning(f"No assignment found for slot {slot_number}: {mapping['team_name']} / {mapping['position_name']}")
    
    # Update slots based on assignment changes
    for slot in config.config_tree.get('slots', []):
        slot_number = slot['slot']
        new_name = current_assignments.get(slot_number)
        old_name = _last_assignment_state.get(slot_number)
        current_slot_name = slot.get('extended_name', '')
        
        # Also check for direct slot mappings (pco_team and pco_position fields)
        if not new_name and slot.get('pco_team') and slot.get('pco_position'):
            # Try to find assignment for this slot's direct mapping
            for service_type_config in service_types_config:
                service_type_id = service_type_config['id']
                
                # Determine which plan to use
                plan = None
                if manual_plan_id:
                    plan = {'id': manual_plan_id, 'service_type_id': service_type_id}
                else:
                    # Try cached schedule first, then fallback to direct API call
                    cached_schedule = get_cached_daily_schedule(service_type_id)
                    if cached_schedule:
                        now = datetime.now(timezone.utc).astimezone()
                        for item in cached_schedule:
                            live_start = date_parser.parse(item['live_start']) if isinstance(item['live_start'], str) else item['live_start']
                            live_end = date_parser.parse(item['live_end']) if isinstance(item['live_end'], str) else item['live_end']
                            if live_start.tzinfo is not None and live_start.tzinfo.utcoffset(live_start) is not None:
                                live_start = live_start.astimezone()
                            if live_end.tzinfo is not None and live_end.tzinfo.utcoffset(live_end) is not None:
                                live_end = live_end.astimezone()
                            
                            if live_start <= now <= live_end:
                                plan = {'id': item['plan_id'], 'service_type_id': service_type_id}
                                break
                        
                        if not plan:
                            # Use first upcoming plan
                            for item in cached_schedule:
                                live_start = date_parser.parse(item['live_start']) if isinstance(item['live_start'], str) else item['live_start']
                                if live_start.tzinfo is not None and live_start.tzinfo.utcoffset(live_start) is not None:
                                    live_start = live_start.astimezone()
                                if live_start > now:
                                    plan = {'id': item['plan_id'], 'service_type_id': service_type_id}
                                    break
                    
                    if not plan:
                        plan = find_live_plan(service_type_id, lead_time_hours)
                        if plan:
                            plan = {'id': plan['id'], 'service_type_id': service_type_id}
                
                if plan:
                    # Build mapping info for direct slot mapping
                    mapping = {
                        'team_name': slot['pco_team'],
                        'position_name': slot['pco_position']
                    }
                    
                    person_name = _find_assignment(plan['id'], service_type_id, mapping)
                    if person_name:
                        new_name = person_name
                        logging.info(f"Found person '{person_name}' for slot {slot_number}")
                        break
        
        # Check if we need to update the slot
        # Update if: new_name is different from old_name OR new_name is different from current_slot_name
        should_update = False
        if new_name != old_name:
            should_update = True
        elif new_name != current_slot_name:
            should_update = True
        
        if should_update:
            update_data = {'slot': slot_number}
            if new_name:
                update_data['extended_name'] = new_name
                logging.info(f"Updating slot {slot_number} to '{new_name}'")
            else:
                update_data['extended_name'] = ''
                logging.info(f"Clearing slot {slot_number} name")
            
            config.update_slot(update_data)
    
    _last_assignment_state = current_assignments


def sync_thread_worker():
    """Worker thread that runs PCO sync periodically."""
    logging.info("PCO sync thread started - will run every 60 minutes")
    
    while not _sync_stop_event.is_set():
        try:
            # Check if we have credentials before attempting sync
            client_id, client_secret = get_pco_credentials()
            if client_id and client_secret:
                logging.info("PCO credentials found, running sync...")
                sync_assignments()
            else:
                logging.warning("No PCO credentials available, skipping sync")
        except Exception as e:
            logging.error(f"PCO sync error: {e}")
        
        # Sleep for 60 minutes, checking for stop event every 5 minutes
        for i in range(12):  # 12 * 5 minutes = 60 minutes
            if _sync_stop_event.is_set():
                break
            time.sleep(300)  # 5 minutes
        
        if not _sync_stop_event.is_set():
            logging.info("PCO sync thread: 60 minutes elapsed, will run sync again")
    
    logging.info("PCO sync thread stopped")


def start_sync_thread():
    """Start the PCO sync thread."""
    global _sync_thread, _last_assignment_state
    
    if _sync_thread and _sync_thread.is_alive():
        logging.warning("PCO sync thread already running")
        return
    
    # Clear the last assignment state when starting the sync thread
    # This ensures that the first sync will update all slots
    _last_assignment_state = {}
    logging.info("Cleared last assignment state for fresh sync")
    
    _sync_stop_event.clear()
    _sync_thread = threading.Thread(target=sync_thread_worker, daemon=True)
    _sync_thread.start()
    logging.info("PCO sync thread started")


def stop_sync_thread():
    """Stop the PCO sync thread."""
    global _sync_thread
    
    if not _sync_thread:
        return
    
    _sync_stop_event.set()
    _sync_thread.join(timeout=5)
    _sync_thread = None
    logging.info("PCO sync thread stopped")


def clear_assignment_state():
    """Clear the last assignment state to force a fresh sync."""
    global _last_assignment_state
    _last_assignment_state = {}
    logging.info("Cleared last assignment state")
