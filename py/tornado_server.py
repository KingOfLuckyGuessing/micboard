import json
import os
import asyncio
import socket
import logging
import secrets
from datetime import datetime, timezone
from urllib.parse import urlencode

from tornado import websocket, web, ioloop, escape
from requests_oauthlib import OAuth2Session

import shure
import config
import discover
import offline
import pco_debug
import planning_center
import pco_endpoints
import google_drive
import micboard


# https://stackoverflow.com/questions/5899497/checking-file-extension
def file_list(extension):
    files = []
    dir_list = os.listdir(config.gif_dir)
    # print(fileList)
    for file in dir_list:
        if file.lower().endswith(extension):
            files.append(file)
    return files

# Its not efficecent to get the IP each time, but for now we'll assume server might have dynamic IP
def localURL():
    if 'local_url' in config.config_tree:
        return config.config_tree['local_url']
    try:
        ip = socket.gethostbyname(socket.gethostname())
        return 'http://{}:{}'.format(ip, config.config_tree['port'])
    except:
        return 'https://micboard.io'
    return 'https://micboard.io'

def micboard_json(network_devices):
    offline_devices = offline.offline_json()
    data = []
    discovered = []
    for net_device in network_devices:
        data.append(net_device.net_json())

    if offline_devices:
        data.append(offline_devices)

    gifs = file_list('.gif')
    jpgs = file_list('.jpg')
    mp4s = file_list('.mp4')
    url = localURL()

    for device in discover.time_filterd_discovered_list():
        discovered.append(device)

    return json.dumps({
        'receivers': data, 'url': url, 'gif': gifs, 'jpg': jpgs, 'mp4': mp4s,
        'config': config.config_tree, 'discovered': discovered
    }, sort_keys=True, indent=4)

class IndexHandler(web.RequestHandler):
    def get(self):
        self.render(config.app_dir('demo.html'))

class AboutHandler(web.RequestHandler):
    def get(self):
        self.render(config.app_dir('static/about.html'))

class JsonHandler(web.RequestHandler):
    def get(self):
        self.set_header('Content-Type', 'application/json')
        # Inject plan_of_day into the json payload
        try:
            # Use the new PCO scheduler instead of the old system
            import pco_scheduler
            scheduler = pco_scheduler.get_scheduler()
            if scheduler:
                # Get all upcoming plans instead of just the current one
                upcoming_plans = scheduler.get_upcoming_plans()
                current_plan = scheduler.get_current_plan()
                
                # Mark which plan is currently active
                for plan in upcoming_plans:
                    plan['is_live'] = (current_plan and plan['plan_id'] == current_plan['plan_id'])
                    plan['is_manual'] = (scheduler.manual_override_plan and 
                                        plan['plan_id'] == scheduler.manual_override_plan['plan_id'])
                    # Merge manual slot overrides if present
                    try:
                        import pco_endpoints
                        ov = pco_endpoints.get_slot_overrides(plan['plan_id'])
                        if ov:
                            sa = plan.get('slot_assignments') or plan.get('names_by_slot') or {}
                            sa.update(ov)
                            plan['slot_assignments'] = sa
                    except Exception as _e:
                        logging.error(f"Override merge failed: {_e}")
                
                # Return all upcoming plans as an array
                plan_of_day = upcoming_plans
            else:
                plan_of_day = []
            
            payload = json.loads(micboard_json(shure.NetworkDevices))
            logging.info(f"JsonHandler: plan_of_day data: {len(plan_of_day)} plans")
            if plan_of_day:
                for plan in plan_of_day:
                    logging.info(f"JsonHandler: Plan {plan.get('plan_id')} - Service Type: {plan.get('service_type_id')}, Title: {plan.get('title')}, Slot assignments: {plan.get('slot_assignments', {})}")
            payload['plan_of_day'] = plan_of_day

            # Additionally, reflect the active plan's assignments into config.slots[].extended_name
            try:
                active_plan = current_plan
                # Fallback: if not set above, recompute
                if 'active_plan' not in locals():
                    try:
                        active_plan = scheduler.get_current_plan() if scheduler else None
                    except Exception:
                        active_plan = None
                if active_plan and payload.get('config') and payload['config'].get('slots'):
                    # Prefer slot_assignments; fallback to names_by_slot if present
                    assignments = active_plan.get('slot_assignments') or active_plan.get('names_by_slot') or {}
                    for slot_obj in payload['config']['slots']:
                        try:
                            s = int(slot_obj.get('slot'))
                        except Exception:
                            continue
                        if s in assignments and assignments[s]:
                            slot_obj['extended_name'] = assignments[s]
            except Exception as _e:
                logging.error(f"Failed to reflect active plan assignments into config slots: {_e}")
            self.write(json.dumps(payload, sort_keys=True, indent=4))
        except Exception as e:
            logging.error(f"JsonHandler: Error getting plan_of_day: {e}")
            # Fallback to original if anything goes wrong
            self.write(micboard_json(shure.NetworkDevices))

class SocketHandler(websocket.WebSocketHandler):
    clients = set()

    def check_origin(self, origin):
        return True

    def open(self):
        self.clients.add(self)

    def on_close(self):
        self.clients.remove(self)

    @classmethod
    def close_all_ws(cls):
        for c in cls.clients:
            c.close()

    @classmethod
    def broadcast(cls, data):
        for c in cls.clients:
            try:
                c.write_message(data)
            except:
                logging.warning("WS Error")

    @classmethod
    def ws_dump(cls):
        out = {}
        if shure.chart_update_list:
            out['chart-update'] = shure.chart_update_list

        if shure.data_update_list:
            out['data-update'] = []
            for ch in shure.data_update_list:
                out['data-update'].append(ch.ch_json_mini())

        if config.group_update_list:
            out['group-update'] = config.group_update_list

        if out:
            data = json.dumps(out)
            cls.broadcast(data)
        del shure.chart_update_list[:]
        del shure.data_update_list[:]
        del config.group_update_list[:]

class SlotHandler(web.RequestHandler):
    def get(self):
        self.write("hi - slot")

    def post(self):
        data = json.loads(self.request.body)
        self.write('{}')
        for slot_update in data:
            config.update_slot(slot_update)
            logging.debug(f"Slot update: {slot_update}")

class ConfigHandler(web.RequestHandler):
    def get(self):
        self.write("hi - slot")

    def post(self):
        data = json.loads(self.request.body)
        logging.debug(f"Config update payload: {data}")
        self.write('{}')
        config.reconfig(data)

class GroupUpdateHandler(web.RequestHandler):
    def get(self):
        self.write("hi - group")

    def post(self):
        data = json.loads(self.request.body)
        config.update_group(data)
        logging.debug(f"Group update: {data}")
        self.write(data)

class MicboardReloadConfigHandler(web.RequestHandler):
    def post(self):
        logging.info("RECONFIG")
        config.reconfig()
        self.write("restarting")


class PCOServiceTypesHandler(web.RequestHandler):
    """Handler for fetching PCO service types."""
    def get(self):
        self.set_header('Content-Type', 'application/json')
        service_types = planning_center.get_service_types()
        
        # Populate the service type names cache with the fetched data
        planning_center.populate_service_type_names_from_data(service_types)
        
        self.write(json.dumps(service_types))


class PCOTeamsHandler(web.RequestHandler):
    """Handler for fetching PCO teams."""
    def get(self):
        service_type_ids = self.get_arguments('service_type_ids[]')
        if not service_type_ids:
            service_type_ids = self.get_argument('service_type_ids', '').split(',')
        
        logging.info(f"PCOTeamsHandler: Received request for service_type_ids: {service_type_ids}")
        
        self.set_header('Content-Type', 'application/json')
        teams = planning_center.get_teams(service_type_ids)
        logging.info(f"PCOTeamsHandler: Returning {len(teams)} teams for service types {service_type_ids}")
        
        
        self.write(json.dumps(teams))


class PCOPositionsHandler(web.RequestHandler):
    """Handler for fetching PCO positions."""
    def get(self):
        service_type_ids = self.get_arguments('service_type_ids[]')
        if not service_type_ids:
            service_type_ids = self.get_argument('service_type_ids', '').split(',')
        team_name = self.get_argument('team_name')
        
        logging.info(f"PCOPositionsHandler: Received request for team '{team_name}' with service_type_ids: {service_type_ids}")
        
        self.set_header('Content-Type', 'application/json')
        positions = planning_center.get_positions(service_type_ids, team_name)
        logging.info(f"PCOPositionsHandler: Returning {len(positions)} positions for team '{team_name}' across service types {service_type_ids}")
        
        
        self.write(json.dumps(positions))


class PCOAuthHandler(web.RequestHandler):
    """Handler for PCO Personal Access Token validation."""
    def get(self):
        # Get current credentials
        client_id, client_secret = planning_center.get_pco_credentials()
        
        
        if not client_id or not client_secret:
            self.set_status(400)
            self.write('PCO credentials not configured. Please save your Client ID and Client Secret first.')
            return
        
        # Test the credentials by making a simple API call
        try:
            session = planning_center.get_pco_session()
            if not session:
                self.set_status(400)
                self.write('Failed to create PCO session')
                return
            
            # Test API call to verify credentials
            response = planning_center._make_pco_request(session, f"{planning_center.PCO_API_BASE}/service_types")
            if not response:
                self.set_status(400)
                self.write('Failed to connect to PCO API')
                return
            
            # Credentials are valid, save to config
            if 'integrations' not in config.config_tree:
                config.config_tree['integrations'] = {}
            if 'planning_center' not in config.config_tree['integrations']:
                config.config_tree['integrations']['planning_center'] = {}
            
            config.config_tree['integrations']['planning_center']['client_id'] = client_id
            config.config_tree['integrations']['planning_center']['client_secret'] = client_secret
            config.save_current_config()
            
            # Initialize the new scheduler with updated credentials instead of legacy threads
            try:
                pco_endpoints.init_pco_scheduler()
                # After initialization, force a refresh so plan-of-day is current immediately
                import pco_scheduler
                scheduler = pco_scheduler.get_scheduler()
                if scheduler:
                    # Use configured service types
                    pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
                    service_types = [st['id'] for st in pco_config.get('service_types', [])]
                    if not service_types:
                        service_types = ['546904', '769651']
                    scheduler.refresh_schedule(service_types)
            except Exception as e:
                logging.error(f"Failed to initialize new PCO scheduler after auth: {e}")
            
            self.write(json.dumps({
                'success': True,
                'message': 'PCO credentials validated successfully!'
            }))
            
        except Exception as e:
            logging.error(f"PCO credential validation error: {e}")
            self.set_status(400)
            self.write(f'Credential validation failed: {str(e)}')


class PCOSyncHandler(web.RequestHandler):
    """Handler for manually triggering PCO sync."""
    def post(self):
        try:
            logging.info("Manual PCO sync triggered")
            # Use the new PCO scheduler instead of the old system
            import pco_scheduler
            scheduler = pco_scheduler.get_scheduler()
            if scheduler:
                # Get service types from config
                pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
                service_types = [st['id'] for st in pco_config.get('service_types', [])]
                if not service_types:
                    service_types = ['546904', '769651']  # Default service types
                
                scheduler.refresh_schedule(service_types)
                
                # Apply current slot assignments
                def update_slot(slot_num, person_name):
                    slot = config.get_slot_by_number(slot_num)
                    if slot:
                        slot['extended_name'] = person_name
                        config.update_slot(slot)
                
                scheduler.apply_current_slot_assignments(update_slot)
                self.write(json.dumps({'status': 'success', 'message': 'PCO sync completed'}))
            else:
                self.write(json.dumps({'status': 'error', 'message': 'PCO scheduler not initialized'}))
        except Exception as e:
            logging.error(f"Manual PCO sync error: {e}")
            self.set_status(500)
            self.write(json.dumps({'status': 'error', 'message': str(e)}))


class PCOResetHandler(web.RequestHandler):
    """Handler for resetting PCO sync state."""
    def post(self):
        try:
            logging.info("PCO sync state reset triggered")
            # Use the new PCO scheduler instead of the old system
            import pco_scheduler
            scheduler = pco_scheduler.get_scheduler()
            if scheduler:
                # Clear all slot names first
                for slot_num in range(1, 7):
                    slot = config.get_slot_by_number(slot_num)
                    if slot:
                        slot['extended_name'] = ''
                        config.update_slot(slot)
                
                # Get service types from config
                pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
                service_types = [st['id'] for st in pco_config.get('service_types', [])]
                if not service_types:
                    service_types = ['546904', '769651']  # Default service types
                
                scheduler.refresh_schedule(service_types)
                
                # Apply current slot assignments
                def update_slot(slot_num, person_name):
                    slot = config.get_slot_by_number(slot_num)
                    if slot:
                        slot['extended_name'] = person_name
                        config.update_slot(slot)
                
                scheduler.apply_current_slot_assignments(update_slot)
                self.write(json.dumps({'status': 'success', 'message': 'PCO sync state reset and sync completed'}))
            else:
                self.write(json.dumps({'status': 'error', 'message': 'PCO scheduler not initialized'}))
        except Exception as e:
            logging.error(f"PCO reset error: {e}")
            self.set_status(500)
            self.write(json.dumps({'status': 'error', 'message': str(e)}))


class ScheduleHandler(web.RequestHandler):
    """Handler for getting service schedules."""
    def get(self):
        try:
            import planning_center
            import config
            
            # Get PCO configuration
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            if not pco_config:
                self.write(json.dumps({'schedules': {}, 'message': 'No PCO configuration'}))
                return
            
            service_types = pco_config.get('service_types', [])
            if not service_types:
                self.write(json.dumps({'schedules': {}, 'message': 'No service types configured'}))
                return
            
            # Serve service schedules from cache
            schedules = {}
            for service_type in service_types:
                service_type_id = service_type['id']
                schedule = planning_center.get_cached_daily_schedule(service_type_id)
                schedules[service_type_id] = schedule
            
            self.write(json.dumps({'schedules': schedules}))
            
        except Exception as e:
            logging.error(f"Schedule handler error: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class LiveScheduleHandler(web.RequestHandler):
    """Handler for getting 24-hour live service schedules."""
    def get(self):
        try:
            import planning_center
            import config
            
            # Get PCO configuration
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            if not pco_config:
                self.write(json.dumps({'live_schedules': {}, 'message': 'No PCO configuration'}))
                return
            
            service_types = pco_config.get('service_types', [])
            lead_time_hours = pco_config.get('lead_time_hours', 2)
            days = int(self.get_argument('days', '1'))
            
            if not service_types:
                self.write(json.dumps({'live_schedules': {}, 'message': 'No service types configured'}))
                return
            
            # Serve from cache to avoid blocking network calls
            live_schedules = {}
            for service_type in service_types:
                service_type_id = service_type['id']
                live_schedule = planning_center.get_cached_daily_schedule(service_type_id)
                live_schedules[service_type_id] = live_schedule
            
            # Add current time for reference
            current_time = datetime.now(timezone.utc).astimezone().isoformat()
            
            self.write(json.dumps({
                'live_schedules': live_schedules,
                'current_time': current_time,
                'lead_time_hours': lead_time_hours,
                'days': days
            }))
            
        except Exception as e:
            logging.error(f"Live schedule handler error: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class PCOTestParametersHandler(web.RequestHandler):
    """Handler for testing different PCO API parameters."""
    def get(self):
        try:
            import planning_center
            import config
            import requests
            from datetime import datetime, timezone, timedelta
            
            # Get PCO configuration
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            if not pco_config:
                self.write(json.dumps({'error': 'No PCO configuration'}))
                return
            
            service_types = pco_config.get('service_types', [])
            if not service_types:
                self.write(json.dumps({'error': 'No service types configured'}))
                return
            
            # Test different parameters
            test_results = {}
            
            for service_type in service_types:
                service_type_id = service_type['id']
                service_type_name = service_type.get('name', f'Service {service_type_id}')
                
                # Get PCO session
                session = planning_center.get_pco_session()
                if not session:
                    test_results[service_type_id] = {'error': 'No PCO session'}
                    continue
                
                # Test different filter parameters
                test_params = {
                    'filter=future': {'filter': 'future', 'order': 'sort_date', 'per_page': 10},
                    'filter=upcoming': {'filter': 'upcoming', 'order': 'sort_date', 'per_page': 10},
                    'no_filter': {'order': 'sort_date', 'per_page': 10},
                    'filter=today': {'filter': 'today', 'order': 'sort_date', 'per_page': 10},
                    'filter=this_week': {'filter': 'this_week', 'order': 'sort_date', 'per_page': 10}
                }
                
                service_results = {}
                
                for param_name, params in test_params.items():
                    try:
                        url = f"{planning_center.PCO_API_BASE}/service_types/{service_type_id}/plans"
                        response = pco_debug.pco_get(session, url, params=params, caller='PCOTestParametersHandler')
                        response.raise_for_status()
                        data = response.json()
                        
                        plans = data.get('data', [])
                        plan_summaries = []
                        
                        for plan in plans[:5]:  # Show first 5 plans
                            plan_summaries.append({
                                'id': plan['id'],
                                'title': plan['attributes']['title'],
                                'dates': plan['attributes']['dates'],
                                'sort_date': plan['attributes']['sort_date']
                            })
                        
                        service_results[param_name] = {
                            'total_plans': len(plans),
                            'plans': plan_summaries,
                            'api_url': f"{url}?{requests.compat.urlencode(params)}"
                        }
                        
                    except Exception as e:
                        service_results[param_name] = {'error': str(e)}
                
                test_results[service_type_id] = {
                    'service_name': service_type_name,
                    'results': service_results
                }
            
            # Add current time for reference
            current_time = datetime.now(timezone.utc).astimezone().isoformat()
            
            self.write(json.dumps({
                'test_results': test_results,
                'current_time': current_time,
                'note': 'Testing different PCO API filter parameters'
            }, indent=2))
            
        except Exception as e:
            logging.error(f"PCO test parameters handler error: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class LiveServiceHandler(web.RequestHandler):
    """Handler for getting current live service information."""
    def get(self):
        try:
            import planning_center
            import config
            
            # Get PCO configuration
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            if not pco_config:
                self.write(json.dumps({'live_service': None, 'message': 'No PCO configuration'}))
                return
            
            service_types = pco_config.get('service_types', [])
            if not service_types:
                self.write(json.dumps({'live_service': None, 'message': 'No service types configured'}))
                return
            
            # Find live service across all configured service types
            live_service = None
            lead_time_hours = pco_config.get('lead_time_hours', 2)
            for service_type in service_types:
                service_type_id = service_type['id']
                live_plan = planning_center.find_live_plan(service_type_id, lead_time_hours)
                if live_plan:
                    # Handle null title by using a fallback
                    title = live_plan.get('title')
                    if not title:
                        # Try to get service type name or use a generic title
                        title = f"Service {service_type_id}"
                    
                    # Convert datetime objects to ISO format strings
                    start_time_str = None
                    end_time_str = None
                    
                    if live_plan.get('start_time'):
                        if hasattr(live_plan['start_time'], 'isoformat'):
                            start_time_str = live_plan['start_time'].isoformat()
                        else:
                            start_time_str = str(live_plan['start_time'])
                    
                    if live_plan.get('end_time'):
                        if hasattr(live_plan['end_time'], 'isoformat'):
                            end_time_str = live_plan['end_time'].isoformat()
                        else:
                            end_time_str = str(live_plan['end_time'])
                    
                    live_service = {
                        'service_type_id': service_type_id,
                        'plan_id': live_plan['id'],
                        'title': title,
                        'dates': live_plan['dates'],
                        'start_time': start_time_str,
                        'end_time': end_time_str
                    }
                    break
            
            if live_service:
                self.write(json.dumps({'live_service': live_service}))
            else:
                self.write(json.dumps({'live_service': None, 'message': 'No live service found'}))
                
        except Exception as e:
            logging.error(f"Live service handler error: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class PCOTestPlansHandler(web.RequestHandler):
    """Handler for testing plans for a specific service type."""
    def get(self):
        try:
            import planning_center
            import config
            
            service_type_id = self.get_argument('service_type_id', '769651')
            
            session = planning_center.get_pco_session()
            if not session:
                self.write(json.dumps({'error': 'No PCO session'}))
                return
            
            # Get plans for this service type
            url = f"{planning_center.PCO_API_BASE}/service_types/{service_type_id}/plans"
            params = {
                'filter': 'future',
                'order': 'sort_date',
                'per_page': 10
            }
            response = pco_debug.pco_get(session, url, params=params, caller='PCOTestPlansHandler')
            response.raise_for_status()
            data = response.json()
            
            plans = data.get('data', [])
            result = {
                'service_type_id': service_type_id,
                'plans': []
            }
            
            for plan in plans:
                plan_id = plan['id']
                plan_title = plan['attributes']['title']
                plan_dates = plan['attributes']['dates']
                
                # Get first service time
                times_url = f"{planning_center.PCO_API_BASE}/service_types/{service_type_id}/plans/{plan_id}/plan_times"
                times_response = pco_debug.pco_get(session, times_url, caller='PCOTestPlansHandler.plan_times')
                times_response.raise_for_status()
                times_data = times_response.json()
                
                if times_data.get('data'):
                    # Find the earliest time in the plan (including rehearsal times)
                    earliest_time = None
                    for time_item in times_data['data']:
                        time_str = time_item['attributes']['starts_at']
                        from dateutil import parser as date_parser
                        from datetime import datetime, timezone, timedelta
                        time_obj = date_parser.parse(time_str)
                        
                        # Convert from UTC to local time if needed
                        if time_obj.tzinfo is not None and time_obj.tzinfo.utcoffset(time_obj) is not None:
                            # Time has timezone info, convert to local time
                            time_obj = time_obj.astimezone()
                        
                        if earliest_time is None or time_obj < earliest_time:
                            earliest_time = time_obj
                    
                    start_time = earliest_time
                    now = datetime.now(timezone.utc)
                    lead_time_hours = 2
                    lead_time_start = start_time - timedelta(hours=lead_time_hours)
                    
                    # Find the next service time or end of day, whichever comes first
                    next_service_time = None
                    for time_item in times_data['data']:
                        time_str = time_item['attributes']['starts_at']
                        time_obj = date_parser.parse(time_str)
                        
                        # Convert from UTC to local time if needed
                        if time_obj.tzinfo is not None and time_obj.tzinfo.utcoffset(time_obj) is not None:
                            # Time has timezone info, convert to local time
                            time_obj = time_obj.astimezone()
                        
                        if time_obj > start_time:  # This is a later time in the same plan
                            if next_service_time is None or time_obj < next_service_time:
                                next_service_time = time_obj
                    
                    # For live services, extend the window to cover the full service duration
                    # Use end of day (midnight of the same day) to ensure the service stays live
                    live_end = start_time.replace(hour=23, minute=59, second=59, microsecond=999999)
                    
                    is_live = lead_time_start <= now <= live_end
                    
                    result['plans'].append({
                        'id': plan_id,
                        'title': plan_title,
                        'dates': plan_dates,
                        'start_time': start_time.isoformat(),
                        'current_time': now.isoformat(),
                        'lead_time_start': lead_time_start.isoformat(),
                        'live_end': live_end.isoformat(),
                        'is_live': is_live
                    })
            
            self.write(json.dumps(result, indent=2))
            
        except Exception as e:
            logging.error(f"PCO test plans error: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class PCORefreshStructureHandler(web.RequestHandler):
    """Handler for refreshing PCO structure (teams and positions)."""
    def post(self):
        try:
            logging.info("PCO structure refresh triggered")
            
            # Get current PCO config
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            service_types = pco_config.get('service_types', [])
            
            if not service_types:
                self.set_status(400)
                self.write(json.dumps({'status': 'error', 'message': 'No service types configured'}))
                return
            
            # Build structure for each service type
            updated_service_types = []
            
            for service_type in service_types:
                service_type_id = service_type['id']
                logging.info(f"Building structure for service type: {service_type_id}")
                
                # Get teams for this service type
                teams = planning_center.get_teams_for_service_type(service_type_id)
                
                # Build updated service type structure with teams and their positions
                updated_service_type = {
                    'id': service_type_id,
                    'name': service_type.get('name', f'Service Type {service_type_id}'),
                    'teams': [],
                    'reuse_rules': service_type.get('reuse_rules', [])
                }
                
                # For each team, get its positions
                for team in teams:
                    team_id = team['id']
                    team_name = team['name']
                    
                    # Get positions for this team in this service type
                    positions = planning_center.get_positions_for_team_in_service_type(service_type_id, team_id)
                    
                    team_structure = {
                        'name': team_name,
                        'id': team_id,
                        'service_type_id': service_type_id,
                        'positions': positions
                    }
                    
                    updated_service_type['teams'].append(team_structure)
                
                updated_service_types.append(updated_service_type)
            
            # Update the config with the new structure
            pco_config['service_types'] = updated_service_types
            config.config_tree['integrations']['planning_center'] = pco_config
            config.save_config()
            
            logging.info("PCO structure refresh completed successfully")
            # After structure refresh, trigger scheduler refresh so plan-of-day rebuilds
            try:
                import pco_scheduler
                scheduler = pco_scheduler.get_scheduler()
                if scheduler:
                    service_types = [st['id'] for st in pco_config.get('service_types', [])]
                    if not service_types:
                        service_types = ['546904', '769651']
                    scheduler.refresh_schedule(service_types)
            except Exception as e:
                logging.error(f"Scheduler refresh after structure update failed: {e}")

            self.write(json.dumps({
                'status': 'success', 
                'message': 'PCO structure refreshed successfully',
                'service_types': updated_service_types
            }))
            
        except Exception as e:
            logging.error(f"PCO structure refresh error: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))



class PCOCacheStatusHandler(web.RequestHandler):
    """Handler for checking PCO schedule cache status."""
    def get(self):
        try:
            import planning_center
            
            # Get cache status
            cache_info = planning_center._schedule_cache
            if cache_info:
                self.write(json.dumps({
                    'status': 'success',
                    'cache_info': {
                        'generated_at': cache_info.get('generated_at', '').isoformat() if cache_info.get('generated_at') else None,
                        'days': cache_info.get('days', 0),
                        'service_types': list(cache_info.get('daily_schedules', {}).keys())
                    }
                }))
            else:
                self.write(json.dumps({
                    'status': 'success',
                    'cache_info': {
                        'generated_at': None,
                        'days': 0,
                        'service_types': []
                    }
                }))
        except Exception as e:
            logging.error(f"PCO cache status error: {e}")
            self.set_status(500)
            self.write(json.dumps({'status': 'error', 'message': str(e)}))


class PCOHealthCheckHandler(web.RequestHandler):
    """Simple health check for PCO endpoints."""
    def get(self):
        try:
            self.write(json.dumps({
                'status': 'success',
                'message': 'PCO endpoints are working',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }))
        except Exception as e:
            logging.error(f"PCO health check error: {e}")
            self.set_status(500)
            self.write(json.dumps({'status': 'error', 'message': str(e)}))


class PCOSetManualPlanHandler(web.RequestHandler):
    """Handler for setting a manual plan selection."""
    def post(self):
        try:
            import planning_center
            
            data = json.loads(self.request.body.decode('utf-8'))
            plan_id = data.get('plan_id')
            
            if not plan_id:
                self.set_status(400)
                self.write(json.dumps({'status': 'error', 'message': 'plan_id is required'}))
                return
            
            # Check if there's currently a scheduled service that should be live
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            service_types = pco_config.get('service_types', [])
            lead_time_hours = pco_config.get('lead_time_hours', 2)
            
            has_scheduled_live = False
            for st in service_types:
                st_id = st['id']
                live_plan = planning_center.find_live_plan(st_id, lead_time_hours)
                if live_plan:
                    has_scheduled_live = True
                    break
            
            if has_scheduled_live:
                self.set_status(400)
                self.write(json.dumps({
                    'status': 'error', 
                    'message': 'Cannot set manual plan while a scheduled service is live'
                }))
                return
            
            # Set the manual plan selection
            planning_center.set_manual_plan_selection(plan_id)
            
            # Just refresh the plan_of_day for the affected service types
            # This is much faster than a full cache refresh
            for st in service_types:
                st_id = st['id']
                try:
                    pod = planning_center._build_plan_of_day_for_service(st_id, lead_time_hours)
                    if pod:
                        planning_center._update_plan_of_day_in_cache(st_id, pod)
                except Exception as e:
                    logging.error(f"Error updating plan_of_day for service type {st_id}: {e}")
            
            self.write(json.dumps({
                'status': 'success',
                'message': f'Manual plan {plan_id} set successfully'
            }))
            
        except Exception as e:
            logging.error(f"PCO set manual plan error: {e}")
            self.set_status(500)
            self.write(json.dumps({'status': 'error', 'message': str(e)}))


class PCOClearManualPlanHandler(web.RequestHandler):
    """Handler for clearing manual plan selection."""
    def post(self):
        try:
            import planning_center
            
            # Clear the manual plan selection
            planning_center.set_manual_plan_selection(None)
            
            # Just refresh the plan_of_day for all service types
            # This is much faster than a full cache refresh
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            service_types = pco_config.get('service_types', [])
            lead_time_hours = pco_config.get('lead_time_hours', 2)
            
            for st in service_types:
                st_id = st['id']
                try:
                    pod = planning_center._build_plan_of_day_for_service(st_id, lead_time_hours)
                    if pod:
                        planning_center._update_plan_of_day_in_cache(st_id, pod)
                except Exception as e:
                    logging.error(f"Error updating plan_of_day for service type {st_id}: {e}")
            
            self.write(json.dumps({
                'status': 'success',
                'message': 'Manual plan selection cleared'
            }))
            
        except Exception as e:
            logging.error(f"PCO clear manual plan error: {e}")
            self.set_status(500)
            self.write(json.dumps({'status': 'error', 'message': str(e)}))


class PCODebugHandler(web.RequestHandler):
    """Handler for debugging PCO plans and assignments."""
    def get(self):
        try:
            import planning_center
            import config
            
            # Get PCO configuration
            pco_config = config.config_tree.get('integrations', {}).get('planning_center', {})
            if not pco_config:
                self.write(json.dumps({'error': 'No PCO configuration found'}))
                return
            
            service_types_config = pco_config.get('service_types', [])
            lead_time_hours = pco_config.get('lead_time_hours', 2)
            
            debug_info = {
                'service_types': [],
                'all_service_types': [],
                'plans': [],
                'assignments': []
            }
            
            # First, get ALL service types to find NORTH GEORGIA REVIVAL
            session = planning_center.get_pco_session()
            if session:
                all_service_types_response = pco_debug.pco_get(session, f"{planning_center.PCO_API_BASE}/service_types", caller='PCODebugHandler')
                all_service_types_response.raise_for_status()
                all_service_types_data = all_service_types_response.json()
                
                for service_type in all_service_types_data.get('data', []):
                    service_type_id = service_type['id']
                    service_type_name = service_type['attributes']['name']
                    
                    # Just list service types for now, don't check plans to avoid rate limiting
                    all_service_info = {
                        'id': service_type_id,
                        'name': service_type_name
                    }
                    debug_info['all_service_types'].append(all_service_info)
            
            # Now process configured service types
            for service_type_config in service_types_config:
                service_type_id = service_type_config['id']
                service_info = {
                    'id': service_type_id,
                    'reuse_rules': service_type_config.get('reuse_rules', [])
                }
                
                # Find live plan
                plan = planning_center.find_live_plan(service_type_id, lead_time_hours)
                if plan:
                    service_info['live_plan'] = plan
                    
                    # Get team members for this plan
                    session = planning_center.get_pco_session()
                    if session:
                        url = f"{planning_center.PCO_API_BASE}/service_types/{service_type_id}/plans/{plan['id']}/team_members"
                        params = {'include': 'person,team_position'}
                        response = pco_debug.pco_get(session, url, params=params, caller='PCODebugHandler.team_members')
                        response.raise_for_status()
                        data = response.json()
                        
                        team_members = []
                        for member in data.get('data', []):
                            status = member['attributes']['status']
                            position_name = member['attributes'].get('team_position_name', '')
                            
                            # Get team name
                            team_ref = member['relationships'].get('team', {}).get('data')
                            team_name = 'Unknown'
                            if team_ref:
                                team_id = team_ref['id']
                                teams_url = f"{planning_center.PCO_API_BASE}/teams/{team_id}"
                                team_response = pco_debug.pco_get(session, teams_url, caller='PCODebugHandler.team')
                                team_response.raise_for_status()
                                team_data = team_response.json()
                                team_name = team_data['data']['attributes']['name']
                            
                            # Get person name
                            person_name = 'Unknown'
                            person_ref = member['relationships']['person']['data']
                            person_key = f"{person_ref['type']}-{person_ref['id']}"
                            
                            included = {}
                            for item in data.get('included', []):
                                key = f"{item['type']}-{item['id']}"
                                included[key] = item
                            
                            person_data = included.get(person_key)
                            if person_data:
                                person_name = person_data['attributes'].get('name') or person_data['attributes'].get('full_name', 'Unknown')
                            
                            team_members.append({
                                'team': team_name,
                                'position': position_name,
                                'person': person_name,
                                'status': status
                            })
                        
                        service_info['team_members'] = team_members
                else:
                    service_info['live_plan'] = None
                
                debug_info['service_types'].append(service_info)
            
            self.write(json.dumps(debug_info, indent=2))
            
        except Exception as e:
            logging.error(f"PCO debug error: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))








class DriveAuthHandler(web.RequestHandler):
    """Handler for Google Drive OAuth authorization."""
    def get(self):
        from google_auth_oauthlib.flow import Flow
        
        # Get current credentials
        client_id, client_secret = google_drive.get_google_credentials()
        
        if not client_id or not client_secret:
            self.set_status(400)
            self.write('''
                <html><body>
                <h2>Google Drive OAuth Setup Required</h2>
                <p>Google OAuth credentials are not configured.</p>
                <p>Please:</p>
                <ol>
                    <li>Go to the Integrations settings</li>
                    <li>Enter your Google Client ID and Client Secret</li>
                    <li>Save the credentials</li>
                    <li>Then try authorizing Google Drive again</li>
                </ol>
                <p><a href="javascript:window.close()">Close this window</a></p>
                </body></html>
            ''')
            return
        
        flow = Flow.from_client_config(
            {
                'web': {
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                    'token_uri': 'https://oauth2.googleapis.com/token',
                    'redirect_uris': [google_drive.GOOGLE_REDIRECT_URI]
                }
            },
            scopes=google_drive.GOOGLE_SCOPES
        )
        
        flow.redirect_uri = google_drive.GOOGLE_REDIRECT_URI
        
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true'
        )
        
        # Store state in cookie for verification
        self.set_secure_cookie('drive_oauth_state', state)
        
        self.redirect(authorization_url)


class DriveCallbackHandler(web.RequestHandler):
    """Handler for Google Drive OAuth callback."""
    def get(self):
        from google_auth_oauthlib.flow import Flow
        
        # Verify state
        state = self.get_secure_cookie('drive_oauth_state')
        if not state or state.decode('utf-8') != self.get_argument('state', ''):
            self.set_status(400)
            self.write('Invalid state parameter')
            return
        
        code = self.get_argument('code', '')
        if not code:
            self.set_status(400)
            self.write('No authorization code received')
            return
        
        try:
            # Get current credentials
            client_id, client_secret = google_drive.get_google_credentials()
            
            flow = Flow.from_client_config(
                {
                    'web': {
                        'client_id': client_id,
                        'client_secret': client_secret,
                        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                        'token_uri': 'https://oauth2.googleapis.com/token',
                        'redirect_uris': [google_drive.GOOGLE_REDIRECT_URI]
                    }
                },
                scopes=google_drive.GOOGLE_SCOPES,
                state=state.decode('utf-8')
            )
            
            flow.redirect_uri = google_drive.GOOGLE_REDIRECT_URI
            flow.fetch_token(code=code)
            
            credentials = flow.credentials
            
            # Save tokens to config
            if 'integrations' not in config.config_tree:
                config.config_tree['integrations'] = {}
            if 'google_drive' not in config.config_tree['integrations']:
                config.config_tree['integrations']['google_drive'] = {}
            
            config.config_tree['integrations']['google_drive']['tokens'] = {
                'access_token': credentials.token,
                'refresh_token': credentials.refresh_token
            }
            config.save_current_config()
            
            # Start sync thread
            google_drive.start_sync_thread()
            
            self.write('''
                <html><body>
                <h2>Google Drive Authorization Successful!</h2>
                <p>You can close this window and return to Micboard.</p>
                <script>
                    if (window.opener) {
                        window.opener.postMessage({type: 'drive_auth_success'}, '*');
                        window.close();
                    }
                </script>
                </body></html>
            ''')
            
        except Exception as e:
            logging.error(f"Drive OAuth error: {e}")
            self.set_status(500)
            self.write(f'Authorization failed: {str(e)}')


class DriveFoldersHandler(web.RequestHandler):
    """Handler for fetching Google Drive folders."""
    def get(self):
        try:
            # Check if OAuth credentials are configured
            client_id, client_secret = google_drive.get_google_credentials()
            if not client_id or not client_secret:
                self.set_status(400)
                self.write(json.dumps({'error': 'Google OAuth credentials not configured. Please set up Google Client ID and Client Secret in the integrations settings.'}))
                return
            
            service = google_drive.get_drive_service()
            if not service:
                self.set_status(401)
                self.write(json.dumps({'error': 'Google Drive not authorized. Please authorize Google Drive access first.'}))
                return
            
            # Query for folders only
            query = "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            results = service.files().list(
                q=query,
                fields="files(id, name, parents)",
                pageSize=1000,
                orderBy="name"
            ).execute()
            
            folders = results.get('files', [])
            
            # Format for frontend
            folder_list = []
            for folder in folders:
                folder_list.append({
                    'id': folder['id'],
                    'name': folder['name'],
                    'has_parent': 'parents' in folder
                })
            
            self.set_header('Content-Type', 'application/json')
            self.write(json.dumps({'folders': folder_list}))
            
        except Exception as e:
            logging.error(f"Error fetching Drive folders: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class DriveFilesStatusHandler(web.RequestHandler):
    """Handler for fetching Google Drive files sync status."""
    def get(self):
        try:
            folder_id = self.get_argument('folder_id', '')
            if not folder_id:
                self.set_status(400)
                self.write(json.dumps({'error': 'Folder ID required'}))
                return
            
            # Check if OAuth credentials are configured
            client_id, client_secret = google_drive.get_google_credentials()
            if not client_id or not client_secret:
                self.set_status(400)
                self.write(json.dumps({'error': 'Google OAuth credentials not configured. Please set up Google Client ID and Client Secret in the integrations settings.'}))
                return
            
            service = google_drive.get_drive_service()
            if not service:
                self.set_status(401)
                self.write(json.dumps({'error': 'Google Drive not authorized. Please authorize Google Drive access first.'}))
                return
            
            # Get files from the specified folder
            query = f"'{folder_id}' in parents and trashed = false and ("
            query += "mimeType contains 'image/' or mimeType = 'video/mp4')"
            
            results = service.files().list(
                q=query,
                fields="files(id, name, mimeType, modifiedTime)",
                pageSize=1000,
                orderBy="name"
            ).execute()
            
            files = results.get('files', [])
            
            # Get current sync state
            last_file_state = google_drive.get_last_file_state()
            downloading_files = google_drive.get_downloading_files()
            
            # Format for frontend
            file_status_list = []
            for file in files:
                file_id = file['id']
                original_name = file['name']
                mime_type = file['mimeType']
                modified_time = file['modifiedTime']
                
                # Determine local name (same logic as sync function)
                csv_mappings = google_drive.get_csv_mapping(service, folder_id)
                if original_name in csv_mappings:
                    local_name = csv_mappings[original_name]
                else:
                    local_name = os.path.splitext(original_name)[0]
                
                # Check sync status
                status = 'not_synced'  # red x
                if local_name.lower() in downloading_files:
                    status = 'downloading'  # rotating arrows
                elif local_name.lower() in last_file_state:
                    if last_file_state[local_name.lower()]['modified'] == modified_time:
                        status = 'synced'  # green check
                    else:
                        status = 'outdated'  # yellow warning
                
                file_status_list.append({
                    'id': file_id,
                    'name': original_name,
                    'local_name': local_name,
                    'mime_type': mime_type,
                    'modified_time': modified_time,
                    'status': status
                })
            
            self.set_header('Content-Type', 'application/json')
            self.write(json.dumps({'files': file_status_list}))
            
        except Exception as e:
            logging.error(f"Error fetching Drive files status: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class IntegrationsConfigHandler(web.RequestHandler):
    """Handler for saving integrations configuration."""
    def get(self):
        self.set_header('Content-Type', 'application/json')
        integrations = config.config_tree.get('integrations', {})
        self.write(json.dumps(integrations))
    
    def post(self):
        data = json.loads(self.request.body)
        
        if 'integrations' not in config.config_tree:
            config.config_tree['integrations'] = {}
        
        # Deep-merge planning_center/google_drive to avoid dropping existing keys (e.g., client_id/secret)
        for key, value in data.items():
            if key in ['planning_center', 'google_drive'] and isinstance(value, dict):
                existing = config.config_tree['integrations'].get(key, {})
                merged = existing.copy()
                merged.update(value)
                config.config_tree['integrations'][key] = merged
            else:
                # Fallback to shallow set for other integration sections
                config.config_tree['integrations'][key] = value
        config.save_current_config()
        
        # No legacy Planning Center threads here; new scheduler runs separately
        
        if 'google_drive' in data:
            google_drive.stop_sync_thread()
            if config.config_tree['integrations']['google_drive'].get('tokens', {}).get('access_token'):
                google_drive.start_sync_thread()
        
        self.write(json.dumps({'status': 'success'}))


class ConfigCleanupHandler(web.RequestHandler):
    """Handler for cleaning up duplicate configuration sections."""
    def post(self):
        try:
            # Trigger cleanup and save
            config.cleanup_duplicate_pco_config()
            config.save_current_config()
            
            self.write(json.dumps({'success': True, 'message': 'Configuration cleaned up successfully'}))
        except Exception as e:
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class OAuthCredentialsHandler(web.RequestHandler):
    """Handler for saving OAuth credentials."""
    def post(self):
        try:
            data = json.loads(self.request.body)
            
            
            # Store credentials in environment variables for this session
            if data.get('pco_client_id'):
                os.environ['PCO_CLIENT_ID'] = data['pco_client_id']
            if data.get('pco_client_secret'):
                os.environ['PCO_CLIENT_SECRET'] = data['pco_client_secret']
            if data.get('google_client_id'):
                os.environ['GOOGLE_CLIENT_ID'] = data['google_client_id']
            if data.get('google_client_secret'):
                os.environ['GOOGLE_CLIENT_SECRET'] = data['google_client_secret']
            
            # Also store in config for persistence
            if 'oauth_credentials' not in config.config_tree:
                config.config_tree['oauth_credentials'] = {}
            
            config.config_tree['oauth_credentials'].update({
                'pco_client_id': data.get('pco_client_id', ''),
                'pco_client_secret': data.get('pco_client_secret', ''),
                'google_client_id': data.get('google_client_id', ''),
                'google_client_secret': data.get('google_client_secret', '')
            })
            
            config.save_current_config()
            
            # Also mirror credentials under integrations.planning_center for UI authorization state
            try:
                if 'integrations' not in config.config_tree:
                    config.config_tree['integrations'] = {}
                if 'planning_center' not in config.config_tree['integrations']:
                    config.config_tree['integrations']['planning_center'] = {}
                if data.get('pco_client_id'):
                    config.config_tree['integrations']['planning_center']['client_id'] = data.get('pco_client_id')
                if data.get('pco_client_secret'):
                    config.config_tree['integrations']['planning_center']['client_secret'] = data.get('pco_client_secret')
                config.save_current_config()
            except Exception as e:
                logging.error(f"Failed to mirror PCO credentials into integrations block: {e}")

            self.write(json.dumps({'success': True}))
        except Exception as e:
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))


class PCOServiceTypeDebugHandler(web.RequestHandler):
    """Handler for debugging specific service type issues."""
    def get(self):
        try:
            import planning_center
            import config
            
            service_type_id = self.get_argument('service_type_id', '')
            if not service_type_id:
                self.write(json.dumps({'error': 'service_type_id parameter required'}))
                return
            
            debug_info = {
                'service_type_id': service_type_id,
                'teams': [],
                'positions': [],
                'error': None
            }
            
            # Get teams for this service type
            try:
                teams = planning_center.get_teams_for_service_type(service_type_id)
                debug_info['teams'] = teams
                logging.info(f"Found {len(teams)} teams for service type {service_type_id}")
                
                # For each team, get positions
                for team in teams:
                    team_id = team['id']
                    team_name = team['name']
                    
                    positions = planning_center.get_positions_for_team_in_service_type(service_type_id, team_id)
                    debug_info['positions'].extend(positions)
                    logging.info(f"Found {len(positions)} positions for team '{team_name}' (ID: {team_id}) in service type {service_type_id}")
                    
            except Exception as e:
                debug_info['error'] = str(e)
                logging.error(f"Error debugging service type {service_type_id}: {e}")
            
            self.set_header('Content-Type', 'application/json')
            self.write(json.dumps(debug_info))
            
        except Exception as e:
            logging.error(f"Error in PCOServiceTypeDebugHandler: {e}")
            self.set_status(500)
            self.write(json.dumps({'error': str(e)}))



class HealthCheckHandler(web.RequestHandler):
    def get(self):
        """Health check endpoint for Docker and monitoring"""
        try:
            # Basic health check - server is responding
            self.write({
                'status': 'healthy',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'version': '2.0.0'
            })
        except Exception as e:
            self.set_status(500)
            self.write({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })

# https://stackoverflow.com/questions/12031007/disable-static-file-caching-in-tornado
class NoCacheHandler(web.StaticFileHandler):
    def set_extra_headers(self, path):
        # Disable cache
        self.set_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')


def twisted():
    # Initialize the new PCO scheduler on startup
    try:
        pco_endpoints.init_pco_scheduler()
    except Exception as e:
        logging.error(f"Failed to initialize PCO scheduler: {e}")
    
    app = web.Application([
        (r'/', IndexHandler),
        (r'/about', AboutHandler),
        (r'/ws', SocketHandler),
        (r'/data.json', JsonHandler),
        (r'/api/group', GroupUpdateHandler),
        (r'/api/slot', SlotHandler),
        (r'/api/config', ConfigHandler),
        (r'/api/integrations', IntegrationsConfigHandler),
        (r'/api/oauth-credentials', OAuthCredentialsHandler),
        (r'/api/health', HealthCheckHandler),
        (r'/api/pco/service-types', PCOServiceTypesHandler),
        (r'/api/pco/teams', PCOTeamsHandler),
        (r'/api/pco/positions', PCOPositionsHandler),
        (r'/api/pco/authorize', PCOAuthHandler),
        (r'/api/pco/sync', PCOSyncHandler),
        (r'/api/pco/reset', PCOResetHandler),
        (r'/api/pco/refresh-structure', PCORefreshStructureHandler),
        # Legacy force-refresh endpoint removed; use /api/pco/refresh-schedule instead
        (r'/api/pco/cache-status', PCOCacheStatusHandler),
        (r'/api/pco/health', PCOHealthCheckHandler),
        (r'/api/pco/set-manual-plan', pco_endpoints.PCOSetManualPlanHandler),
        (r'/api/pco/clear-manual-plan', pco_endpoints.PCOClearManualPlanHandler),
        (r'/api/pco/debug', PCODebugHandler),
        (r'/api/pco/debug-service-type', PCOServiceTypeDebugHandler),
        
        # New simplified PCO endpoints
        (r'/api/pco/upcoming-plans', pco_endpoints.PCOUpcomingPlansHandler),
        (r'/api/pco/current-plan', pco_endpoints.PCOCurrentPlanHandler),
        (r'/api/pco/refresh-schedule', pco_endpoints.PCORefreshScheduleHandler),
        (r'/api/pco/test-plans', PCOTestPlansHandler),
        (r'/api/schedule', ScheduleHandler),
        (r'/api/live-schedule', LiveScheduleHandler),
        (r'/api/pco/test-parameters', PCOTestParametersHandler),
        (r'/api/live-service', LiveServiceHandler),
        (r'/api/pco/slot-overrides', pco_endpoints.PCOSlotOverridesHandler),
        (r'/api/drive/authorize', DriveAuthHandler),
        (r'/api/drive/callback', DriveCallbackHandler),
        (r'/api/drive/folders', DriveFoldersHandler),
        (r'/api/drive/files-status', DriveFilesStatusHandler),
        (r'/api/config/cleanup', ConfigCleanupHandler),
        # (r'/restart/', MicboardReloadConfigHandler),
        (r'/static/(.*)', web.StaticFileHandler, {'path': config.app_dir('static')}),
        (r'/bg/(.*)', NoCacheHandler, {'path': config.get_gif_dir()})
    ], cookie_secret=os.environ.get('COOKIE_SECRET', secrets.token_hex(32)))
    # https://github.com/tornadoweb/tornado/issues/2308
    asyncio.set_event_loop(asyncio.new_event_loop())
    app.listen(config.web_port())
    ioloop.PeriodicCallback(SocketHandler.ws_dump, 50).start()
    
    # Disable legacy Planning Center background threads; the new scheduler is used instead
    try:
        pco_client_id, pco_client_secret = planning_center.get_pco_credentials()
        if pco_client_id and pco_client_secret:
            logging.info("Using new PCO scheduler; legacy Planning Center threads are disabled")
        else:
            logging.info("PCO credentials not configured; new scheduler will initialize when available")
    except Exception as e:
        logging.error(f"Planning Center credential check error: {e}")
    
    if config.config_tree.get('integrations', {}).get('google_drive', {}).get('tokens', {}).get('access_token'):
        google_drive.start_sync_thread()
    
    ioloop.IOLoop.instance().start()
