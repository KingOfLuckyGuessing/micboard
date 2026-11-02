"""Download Planning Center profile pictures into the backgrounds folder.

Why this exists
---------------
The tile-background code in ``js/gif.js`` does not take a picture URL at
all: for a tile whose displayed name is e.g. "John Smith", in IMG mode it
looks for a file literally named ``john smith.jpg`` in the backgrounds
folder (``config.get_gif_dir()``, served at ``/bg/``). So the way to put
a scheduled person's face on their tile is to save their PCO photo
under exactly that name.

"Selected" here means: the person is mapped to a slot in a fetched plan
(see ``PCOScheduler._map_assignments_to_slots``). Merely being a team
member in a plan is not enough -- only mapped people ever appear as a
tile name, so only they get a download. Callers pass only mapped
assignments in; this module does not decide mapping itself.

Where the link comes from
--------------------------
PCO's Services ``Person`` object (the ``included`` person in a
``team_members?include=person,team_position`` response) carries
``photo_url`` (falling back to ``photo_thumbnail_url``). The scheduler
extracts that into each assignment's ``photo_url`` field; this module
just downloads the URL it is given. It never invents a URL and never
queries the People API separately.

Safety notes
------------
- PCO credentials are only sent to PCO hosts. A photo URL on a CDN
  (the usual case) is fetched with a plain, unauthenticated request,
  so the Basic-auth token never leaks to a third party.
- Downloads are capped (10s timeout, 10MB) and written atomically
  (temp file + replace) so the web server never serves a half-written
  image.
- A small sidecar, ``.pco_avatar_urls.json`` in the backgrounds folder,
  records which URL each file came from. A re-download is skipped when
  the URL is unchanged, and a changed URL re-downloads. The sidecar's
  extension is ``.json``, so ``tornado_server.file_list()`` never lists
  it as a background.
- A manually placed background for a person who is *not* selected is
  never touched. A selected person's file *is* overwritten when their
  PCO photo URL changes -- that is the point of the feature.
"""

import json
import logging
import os
import threading
from urllib.parse import urlparse

import requests

MAX_PHOTO_BYTES = 10 * 1024 * 1024
_STATE_FILENAME = '.pco_avatar_urls.json'

_download_thread = None
_download_lock = threading.Lock()


def photo_filename(person_name):
    """Return the backgrounds-folder filename for a person, or None.

    This must match ``js/gif.js`` exactly: the tile's displayed name,
    lowercased, plus ``.jpg``. Path separators are replaced so a name
    can never escape the backgrounds folder, and leading dots are
    stripped so a name can never become a hidden file.
    """
    if not person_name:
        return None
    name = person_name.strip().lower()
    name = name.replace('/', '_').replace('\\', '_')
    name = name.lstrip('.')
    if not name:
        return None
    return name + '.jpg'


def _is_pco_host(url):
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host == 'planningcenteronline.com' or host.endswith('.planningcenteronline.com')


def _load_state(backgrounds_dir):
    path = os.path.join(backgrounds_dir, _STATE_FILENAME)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(backgrounds_dir, state):
    path = os.path.join(backgrounds_dir, _STATE_FILENAME)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def download_person_photo(person_name, photo_url, backgrounds_dir,
                          session=None, fetcher=None):
    """Download one person's photo into the backgrounds folder.

    Returns True if the file is present afterwards (downloaded just
    now, or already present from the same URL), False otherwise.
    ``fetcher`` is a test hook: a callable ``(url, timeout)`` returning
    an object with ``status_code``, ``headers`` and ``content``.
    """
    filename = photo_filename(person_name)
    if not filename:
        return False
    if not photo_url or not isinstance(photo_url, str):
        logging.info(f"No photo URL for '{person_name}', skipping photo download")
        return False
    parsed = urlparse(photo_url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        logging.warning(f"Ignoring non-HTTP photo URL for '{person_name}': {photo_url}")
        return False

    try:
        os.makedirs(backgrounds_dir, exist_ok=True)
    except Exception as e:
        logging.error(f"Cannot create backgrounds dir {backgrounds_dir}: {e}")
        return False

    target = os.path.join(backgrounds_dir, filename)
    state = _load_state(backgrounds_dir)
    if state.get(filename) == photo_url and os.path.exists(target):
        logging.debug(f"Photo for '{person_name}' already up to date, skipping")
        return True

    try:
        if fetcher is not None:
            response = fetcher(photo_url, timeout=10)
        elif session is not None and _is_pco_host(photo_url):
            response = session.get(photo_url, timeout=10)
        else:
            # Plain request on purpose: never send PCO credentials to a CDN.
            response = requests.get(photo_url, timeout=10)
    except Exception as e:
        logging.error(f"Failed to download photo for '{person_name}' from {photo_url}: {e}")
        return False

    try:
        status = getattr(response, 'status_code', None)
        if status != 200:
            logging.warning(f"Photo download for '{person_name}' returned HTTP {status}")
            return False
        content_type = ''
        headers = getattr(response, 'headers', {}) or {}
        # headers may be a plain dict or a case-insensitive mapping
        try:
            content_type = headers.get('Content-Type', '') or headers.get('content-type', '')
        except Exception:
            content_type = ''
        if content_type and not content_type.lower().startswith('image/'):
            logging.warning(f"Photo download for '{person_name}' is not an image ({content_type})")
            return False
        content = getattr(response, 'content', b'')
        if not content:
            logging.warning(f"Photo download for '{person_name}' was empty")
            return False
        if len(content) > MAX_PHOTO_BYTES:
            logging.warning(f"Photo download for '{person_name}' exceeds {MAX_PHOTO_BYTES} bytes")
            return False
        tmp = target + '.tmp'
        with open(tmp, 'wb') as f:
            f.write(content)
        os.replace(tmp, target)
    except Exception as e:
        logging.error(f"Failed to save photo for '{person_name}': {e}")
        return False

    try:
        state[filename] = photo_url
        _save_state(backgrounds_dir, state)
    except Exception as e:
        # The photo itself is saved; a stale sidecar just means a
        # re-download next time, so don't fail the whole operation.
        logging.warning(f"Could not update photo state for '{person_name}': {e}")
    logging.info(f"Downloaded photo for '{person_name}' -> {target}")
    return True


def sync_assignment_photos(assignments, backgrounds_dir, session=None, fetcher=None):
    """Download photos for mapped assignments.

    ``assignments`` is a list of dicts with ``person_name`` and
    ``photo_url``. Entries without a name or URL are skipped. If two
    people share a tile filename (same name, different people), the
    first one wins and the collision is logged, rather than silently
    overwriting. Returns ``{filename: bool}``.
    """
    results = {}
    seen = set()
    for assignment in assignments or []:
        person_name = (assignment or {}).get('person_name')
        photo_url = (assignment or {}).get('photo_url')
        filename = photo_filename(person_name)
        if not filename or not photo_url:
            continue
        if filename in seen:
            logging.warning(f"Skipping photo for '{person_name}': "
                            f"another person already uses '{filename}'")
            continue
        seen.add(filename)
        results[filename] = download_person_photo(
            person_name, photo_url, backgrounds_dir,
            session=session, fetcher=fetcher)
    return results


def sync_assignment_photos_async(assignments, backgrounds_dir, session=None):
    """Kick off ``sync_assignment_photos`` in a daemon thread.

    Used from the scheduler so a slow photo download never blocks the
    Tornado IOLoop (and therefore never blocks ``/data.json`` polling).
    If a download thread is already running, the new request is
    dropped -- the running thread covers the same people, and the next
    scheduler refresh will pick up anything it missed.
    """
    global _download_thread
    with _download_lock:
        if _download_thread is not None and _download_thread.is_alive():
            logging.info("Photo download already in progress, skipping duplicate request")
            return False
        _download_thread = threading.Thread(
            target=sync_assignment_photos,
            args=(assignments, backgrounds_dir),
            kwargs={'session': session},
            daemon=True)
        _download_thread.start()
        return True
