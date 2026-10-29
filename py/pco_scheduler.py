"""
Simplified Planning Center Scheduler
Handles fetching plans, determining live times, and managing slot assignments
"""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import requests
from dateutil import parser as date_parser

import pco_debug

# PCO API Configuration
PCO_API_BASE = 'https://api.planningcenteronline.com/services/v2'

class PCOScheduler:
    """Manages Planning Center schedule and slot assignments"""
    
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = None
        
        # Cache for all upcoming plans across all service types
        self.upcoming_plans = []  # List of plan objects sorted by live_time
        self.current_live_plan = None
        self.manual_override_plan = None
        
        # Slot mappings (position name -> slot number)
        self.slot_mappings = {}
        
        # Threading
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._scheduler_thread = None
        
        # Throttle how often we recompute live status (seconds)
        self._live_check_interval_seconds = 300  # 5 minutes
        
        # Initialize session
        self._init_session()
    
    def _normalize_position_name(self, name: str) -> str:
        """Normalize position names for reliable matching (e.g., 'MIc 1' -> 'mic1')."""
        if not name:
            return ''
        lowered = name.strip().lower()
        # remove spaces, hyphens and underscores for robust matching
        collapsed = ''.join(ch for ch in lowered if ch.isalnum())
        return collapsed

    def _init_session(self):
        """Initialize PCO API session"""
        self.session = requests.Session()
        self.session.auth = (self.client_id, self.client_secret)
        self.session.headers.update({
            'X-PCO-API-Version': '2023-08-01'
        })
    
    def _make_request(self, url: str, params: Dict = None) -> Optional[requests.Response]:
        """Make API request with error handling.

        The request/response (including the body, when PCO_DEBUG is on)
        is logged by ``pco_debug.pco_get``. This wrapper itself never
        changes the response: 404 still returns None here, and other
        errors still return None after logging, exactly as before.
        """
        try:
            response = pco_debug.pco_get(self.session, url, params=params, timeout=10,
                                         caller='pco_scheduler._make_request')
            if response.status_code == 404:
                logging.info(f"Resource not found (404): {url}")
                return None
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            logging.error(f"Request timeout: {url}")
            return None
        except Exception as e:
            logging.error(f"Request error: {e}")
            return None
    
    def fetch_upcoming_plans(self, service_types: List[str], plans_per_type: int = 2) -> List[Dict]:
        """
        Fetch upcoming plans for all configured service types
        Returns a unified list sorted by live time
        """
        all_plans = []
        
        for service_type_id in service_types:
            logging.info(f"Fetching plans for service type {service_type_id}")
            
            # Get next N plans using 'future' filter
            url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans"
            params = {
                'filter': 'future',
                'order': 'sort_date',
                'per_page': plans_per_type
            }
            
            response = self._make_request(url, params)
            if not response:
                continue
            
            plans_data = response.json()
            
            for plan in plans_data.get('data', []):
                plan_id = plan['id']
                
                # Get service times for this plan
                times_url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_times"
                times_response = self._make_request(times_url)
                if not times_response:
                    continue
                
                times_data = times_response.json()
                service_times = times_data.get('data', [])
                
                if not service_times:
                    continue
                
                # Find earliest service time
                earliest_time = None
                for time_item in service_times:
                    time_str = time_item['attributes']['starts_at']
                    time_obj = date_parser.parse(time_str)
                    
                    # Convert to local timezone
                    if time_obj.tzinfo:
                        time_obj = time_obj.astimezone()
                    
                    if earliest_time is None or time_obj < earliest_time:
                        earliest_time = time_obj
                
                if not earliest_time:
                    continue
                
                # Calculate live time (service time - lead time)
                # Get lead time from config (default 2 hours)
                lead_time_hours = self.get_lead_time(service_type_id)
                live_time = earliest_time - timedelta(hours=lead_time_hours)
                
                # Get assignments for this plan
                assignments = self._fetch_plan_assignments(service_type_id, plan_id)
                
                # Build plan object
                plan_obj = {
                    'plan_id': plan_id,
                    'service_type_id': service_type_id,
                    'service_type_name': self.get_service_type_name(service_type_id),
                    'title': plan['attributes'].get('title') or plan['attributes'].get('dates'),
                    'dates': plan['attributes'].get('dates'),
                    'service_time': earliest_time.isoformat(),
                    'live_time': live_time.isoformat(),
                    'assignments': assignments,
                    'slot_assignments': {}  # Will be populated by _map_assignments_to_slots
                }
                
                # Map assignments to slots
                plan_obj['slot_assignments'] = self._map_assignments_to_slots(assignments, service_type_id)
                
                all_plans.append(plan_obj)
                logging.info(f"Added plan {plan_id} for {plan_obj['dates']} (live at {live_time})")
        
        # Sort all plans by live time
        all_plans.sort(key=lambda x: date_parser.parse(x['live_time']))
        
        return all_plans
    
    def _fetch_plan_assignments(self, service_type_id: str, plan_id: str) -> List[Dict]:
        """Fetch assignments using team_members endpoint with includes to get individual position names"""
        # Use team_members endpoint with includes to get individual position names
        url = f"{PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/team_members"
        params = {'include': 'person,team_position'}
        response = self._make_request(url, params)
        
        if not response:
            logging.warning(f"No assignments found for plan {plan_id}")
            return []
        
        data = response.json()
        assignments = []
        
        # Build lookup for included data
        included = {}
        for item in data.get('included', []):
            key = f"{item['type']}-{item['id']}"
            included[key] = item
        
        for item in data.get('data', []):
            # Get person name
            person_ref = item.get('relationships', {}).get('person', {}).get('data', {})
            if person_ref and 'type' in person_ref and 'id' in person_ref:
                person_key = f"{person_ref['type']}-{person_ref['id']}"
                person_data = included.get(person_key, {})
                person_name = person_data.get('attributes', {}).get('name', '') or person_data.get('attributes', {}).get('full_name', '')
            else:
                person_name = ''

            # Prefer PlanPerson.attributes.team_position_name when present
            position_name = item.get('attributes', {}).get('team_position_name', '') or ''

            # Fallback to included TeamPosition.attributes.name
            if not position_name:
                position_ref = item.get('relationships', {}).get('team_position', {}).get('data', {})
                if position_ref and 'type' in position_ref and 'id' in position_ref:
                    position_key = f"{position_ref['type']}-{position_ref['id']}"
                    position_data = included.get(position_key, {})
                    position_name = position_data.get('attributes', {}).get('name', '')

            # Get status
            status = item.get('attributes', {}).get('status', 'C')

            assignments.append({
                'person_name': person_name,
                'position_name': position_name,
                'status': status
            })
        
        logging.info(f"Found {len(assignments)} team members for plan {plan_id}")
        # Log a sample of first few to verify mapping at runtime
        for sample in assignments[:8]:
            logging.info(f"Assignment sample: person='{sample.get('person_name')}', position='{sample.get('position_name')}', status='{sample.get('status')}'")
        return assignments
    
    def _map_assignments_to_slots(self, assignments: List[Dict], service_type_id: str = None) -> Dict[int, str]:
        """
        Map assignments to slot numbers based on position names and configured mappings
        Returns dict of {slot_number: person_name}
        """
        slot_assignments = {}
        
        logging.info(f"_map_assignments_to_slots: Processing {len(assignments)} assignments for service_type_id={service_type_id}")
        
        # Get configured mappings for this service type
        configured_mappings = self._get_configured_mappings(service_type_id)
        
        for assignment in assignments:
            position_name = assignment.get('position_name', '')
            person_name = assignment.get('person_name', '')
            
            if not position_name or not person_name:
                continue
            
            # Check if this position is mapped to a slot using service-type-specific mapping
            slot_number = self._get_slot_for_position_with_service_type(position_name, service_type_id, configured_mappings)
            if slot_number:
                slot_assignments[slot_number] = person_name
                logging.info(f"Mapped {person_name} ({position_name}) to slot {slot_number}")
        
        logging.info(f"_map_assignments_to_slots: Final slot assignments: {slot_assignments}")
        return slot_assignments
    
    def _get_configured_mappings(self, service_type_id: str = None) -> Dict[str, int]:
        """
        Get configured position name to slot mappings for a service type
        """
        import config
        
        mappings = {}
        
        # Get PCO configuration
        pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
        service_types = pco_config.get('service_types', [])
        
        logging.info(f"_get_configured_mappings: Looking for mappings for service_type_id={service_type_id}")
        logging.info(f"_get_configured_mappings: Found {len(service_types)} service types in config")
        
        for st in service_types:
            st_id = st.get('id')
            logging.info(f"_get_configured_mappings: Processing service type {st_id}")
            
            # If service_type_id is specified, only get mappings for that service type
            if service_type_id and st_id != service_type_id:
                logging.info(f"_get_configured_mappings: Skipping service type {st_id} (not {service_type_id})")
                continue
            
            # Get mappings from reuse_rules (name-based)
            reuse_rules = st.get('reuse_rules', [])
            logging.info(f"_get_configured_mappings: Found {len(reuse_rules)} reuse rules for service type {st_id}")
            
            for rule in reuse_rules:
                position_name = rule.get('position_name')
                slot_number = rule.get('slot')
                if position_name and slot_number:
                    normalized_name = self._normalize_position_name(position_name)
                    mappings[normalized_name] = slot_number
                    logging.info(f"_get_configured_mappings: Added mapping {position_name} (norm='{normalized_name}') -> slot {slot_number}")
            
            # Get mappings from teams/positions (ID-based, but we need to map by name)
            # This is more complex as we need to fetch position names from PCO API
            # For now, we'll rely on the reuse_rules which should be configured properly
        
        # If no service-specific mappings found, try to get mappings from all service types
        if not mappings and service_type_id:
            logging.info(f"_get_configured_mappings: No service-specific mappings found for {service_type_id}, trying all service types")
            for st in service_types:
                for rule in st.get('reuse_rules', []):
                    position_name = rule.get('position_name')
                    slot_number = rule.get('slot')
                    if position_name and slot_number:
                        normalized_name = self._normalize_position_name(position_name)
                        mappings[normalized_name] = slot_number
                        logging.info(f"_get_configured_mappings: Added fallback mapping {position_name} (norm='{normalized_name}') -> slot {slot_number}")
        
        # If still no mappings found, fall back to global mappings
        if not mappings:
            logging.info(f"_get_configured_mappings: No configured mappings found, falling back to global mappings")
            mappings = self.slot_mappings.copy()
        
        logging.info(f"_get_configured_mappings: Returning {len(mappings)} total mappings: {mappings}")
        return mappings
    
    def _get_slot_for_position(self, position_name: str, configured_mappings: Dict[str, int] = None) -> Optional[int]:
        """
        Get slot number for a position name
        Checks configured mappings and fallback patterns
        """
        # Normalize inputs and use normalized mapping keys
        normalized = self._normalize_position_name(position_name)
        mappings = configured_mappings or {self._normalize_position_name(k): v for k, v in self.slot_mappings.items()}

        # First check configured mappings
        if normalized in mappings:
            return mappings[normalized]

        # Check for generic "micN" pattern
        if normalized.startswith('mic'):
            num_str = normalized[3:]
            if num_str.isdigit():
                mic_number = int(num_str)
                if 1 <= mic_number <= 32:
                    return mic_number

        return None

    def _get_slot_for_position_with_service_type(self, position_name: str, service_type_id: str, configured_mappings: Dict[str, int] = None) -> Optional[int]:
        """
        Get slot number for a position name using service-type-specific mappings
        """
        import config
        
        # First try to find a mapping that matches both position name and service type
        pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
        service_types = pco_config.get('service_types', [])
        normalized_incoming = self._normalize_position_name(position_name)
        
        for st in service_types:
            if st.get('id') != service_type_id:
                continue
            
            # Check reuse rules for this service type
            for rule in st.get('reuse_rules', []):
                rule_name = rule.get('position_name')
                if self._normalize_position_name(rule_name) == normalized_incoming:
                    slot_number = rule.get('slot')
                    if slot_number:
                        logging.info(f"Found service-type-specific mapping: {position_name} -> slot {slot_number} for service type {service_type_id}")
                        return slot_number
            
            # Check teams/positions for this service type
            for team in st.get('teams', []):
                for position in team.get('positions', []):
                    if self._normalize_position_name(position.get('name')) == normalized_incoming:
                        slot_number = position.get('slot')
                        if slot_number:
                            logging.info(f"Found team/position mapping: {position_name} -> slot {slot_number} for service type {service_type_id}")
                            return slot_number
        
        # Fall back to generic position name matching
        return self._get_slot_for_position(position_name, configured_mappings)
    
    def get_lead_time(self, service_type_id: str) -> int:
        """Get lead time for a service type (default 2 hours)"""
        # This could be configured per service type
        return 2
    
    def get_service_type_name(self, service_type_id: str) -> str:
        """Get friendly name for service type by fetching from PCO API"""
        import planning_center
        return planning_center.get_service_type_name(service_type_id)
    
    def refresh_schedule(self, service_types: List[str]):
        """Refresh the schedule cache"""
        logging.info("Refreshing PCO schedule...")
        
        with self._lock:
            self.upcoming_plans = self.fetch_upcoming_plans(service_types)
            
            # Check if we should be live
            self._update_live_status()
        
        logging.info(f"Schedule refreshed: {len(self.upcoming_plans)} plans in queue")
    
    def _update_live_status(self):
        """Update which plan should be live based on current time"""
        now = datetime.now(timezone.utc).astimezone()
        
        # Store previous live plan to detect changes
        previous_live_plan = self.current_live_plan
        
        # Reset current live plan
        self.current_live_plan = None
        
        # Find if any plan should be live
        for i, plan in enumerate(self.upcoming_plans):
            live_time = date_parser.parse(plan['live_time'])
            service_time = date_parser.parse(plan['service_time'])
            
            # Calculate the end of the live window
            # Live window ends at: end of service day OR next service's live time (whichever comes first)
            end_of_service_day = service_time.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Check if there's a next service
            next_live_time = None
            if i + 1 < len(self.upcoming_plans):
                next_plan = self.upcoming_plans[i + 1]
                next_live_time = date_parser.parse(next_plan['live_time'])
            
            # Live window ends at the earlier of: end of service day or next service's live time
            if next_live_time and next_live_time < end_of_service_day:
                live_window_end = next_live_time
            else:
                live_window_end = end_of_service_day
            
            # Check if we're in this plan's live window
            if live_time <= now <= live_window_end:
                self.current_live_plan = plan
                logging.info(f"Plan {plan['plan_id']} is live (window: {live_time} to {live_window_end})")
                break
        
        # If the live plan changed, apply slot assignments
        if self.current_live_plan != previous_live_plan:
            if self.current_live_plan:
                logging.info(f"Live plan changed to {self.current_live_plan['plan_id']}, applying slot assignments")
                # Don't apply slot assignments here - let the main server thread handle it
                # This prevents deadlocks in the background scheduler thread
            else:
                logging.info("No plan is live, clearing slot assignments")
                # Don't clear slot assignments here - let the main server thread handle it
    
    def get_current_plan(self) -> Optional[Dict]:
        """
        Get the plan that should currently be active
        Returns manual override if set and no scheduled plan is live
        """
        with self._lock:
            # Scheduled plan always takes precedence
            if self.current_live_plan:
                return self.current_live_plan
            
            # Return manual override if no scheduled plan is live
            if self.manual_override_plan:
                return self.manual_override_plan
            
            return None
    
    def set_manual_plan(self, plan_id: str) -> bool:
        """
        Set a manual plan override
        Returns False if a scheduled plan is currently live
        """
        with self._lock:
            # Don't allow manual override during scheduled live time
            if self.current_live_plan:
                logging.warning("Cannot set manual plan - scheduled plan is live")
                return False
            
            # Find the plan in our cache
            for plan in self.upcoming_plans:
                if plan['plan_id'] == plan_id:
                    self.manual_override_plan = plan
                    logging.info(f"Manual plan set: {plan_id}")
                    return True
            
            logging.error(f"Plan {plan_id} not found in cache")
            return False
    
    def clear_manual_plan(self):
        """Clear manual plan override"""
        with self._lock:
            self.manual_override_plan = None
            logging.info("Manual plan cleared")
    
    def get_upcoming_plans(self) -> List[Dict]:
        """Get list of all upcoming plans"""
        with self._lock:
            return self.upcoming_plans.copy()
    
    def update_slot_mappings(self, mappings: Dict[str, int]):
        """Update position name to slot number mappings"""
        with self._lock:
            self.slot_mappings = mappings
            logging.info(f"Updated slot mappings: {mappings}")
    
    def apply_current_slot_assignments(self, config_update_func):
        """
        Apply current plan's slot assignments to the configuration
        config_update_func should be a function that takes (slot_number, person_name)
        """
        current_plan = self.get_current_plan()
        if not current_plan:
            logging.info("No current plan to apply slot assignments")
            return
        
        slot_assignments = current_plan.get('slot_assignments', {}).copy()
        # Merge in any manual overrides for this plan so they persist
        try:
            import pco_endpoints
            ov = pco_endpoints.get_slot_overrides(current_plan.get('plan_id')) if current_plan else {}
            if ov:
                slot_assignments.update(ov)
        except Exception as _e:
            logging.error(f"apply_current_slot_assignments: override merge failed: {_e}")
        
        # First, clear all slot names
        for slot_num in range(1, 7):
            config_update_func(slot_num, '')
        
        # Then apply current assignments
        for slot_num, person_name in slot_assignments.items():
            config_update_func(slot_num, person_name)
            logging.info(f"Applied slot {slot_num}: {person_name}")
    
    def start_scheduler(self, service_types: List[str]):
        """Start the background scheduler thread"""
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            logging.warning("Scheduler already running")
            return
        
        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_worker,
            args=(service_types,),
            daemon=True
        )
        self._scheduler_thread.start()
        logging.info("PCO scheduler started")
    
    def stop_scheduler(self):
        """Stop the background scheduler thread"""
        self._stop_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        logging.info("PCO scheduler stopped")
    
    def _scheduler_worker(self, service_types: List[str]):
        """Background worker that refreshes schedule and checks live status"""
        # Initial refresh
        self.refresh_schedule(service_types)
        
        last_refresh_date = datetime.now().date()
        seconds_since_last_live_check = 0
        
        while not self._stop_event.is_set():
            # Check if we need to refresh at midnight
            current_date = datetime.now().date()
            current_time = datetime.now().time()
            
            # Refresh at midnight (00:00:01)
            if current_date != last_refresh_date and current_time >= datetime.strptime("00:00:01", "%H:%M:%S").time():
                self.refresh_schedule(service_types)
                last_refresh_date = current_date
            
            # Update live status on the configured interval
            if seconds_since_last_live_check >= self._live_check_interval_seconds:
                with self._lock:
                    self._update_live_status()
                seconds_since_last_live_check = 0
            
            # Sleep for 60 seconds total in 5-second increments to allow prompt shutdown
            for _ in range(12):
                if self._stop_event.is_set():
                    break
                time.sleep(5)
                seconds_since_last_live_check += 5
        
        logging.info("Scheduler worker stopped")


# Global scheduler instance
_scheduler: Optional[PCOScheduler] = None

def init_scheduler(client_id: str, client_secret: str) -> PCOScheduler:
    """Initialize the global scheduler instance"""
    global _scheduler
    _scheduler = PCOScheduler(client_id, client_secret)
    return _scheduler

def get_scheduler() -> Optional[PCOScheduler]:
    """Get the global scheduler instance"""
    return _scheduler
