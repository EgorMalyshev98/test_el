from instagrapi import Client
from instagrapi.exceptions import LoginRequired
from loguru import logger
from pathlib import Path

USERNAME = 'testaccel_1'
PASSWORD = '4meUNhsigbUWaa'


def login_user():
    """
    Attempts to login to Instagram using either the provided session information
    or the provided username and password.
    """

    cl = Client()
    cl.delay_range = [1, 3]
    session_path = Path('session.json')
    
    if session_path.exists():
        try:
            session = cl.load_settings(session_path)
            cl.set_settings(session)
            cl.login(USERNAME, PASSWORD)

            try:
                cl.get_timeline_feed()
                logger.debug("login via session")
                return cl
            except LoginRequired:
                logger.warning("Session is invalid, need to login via username and password")

                old_session = cl.get_settings()
                cl.set_settings({})
                cl.set_uuids(old_session["uuids"])
                cl.login(USERNAME, PASSWORD)
                logger.debug("login via username and password")
                return cl
      
        except Exception as e:
            logger.info("Couldn't login user using session information: %s" % e)
    else:
        cl.login(USERNAME, PASSWORD)
        cl.dump_settings(session_path)
        logger.debug("login via username and password with dump settings")

        return cl


def main():
    cl = login_user()
    user_id = cl.user_id_from_username('teona.kapri')
    media_list = cl.user_medias_v1(user_id, amount=5)
    
    for media in media_list:
        print(f"ID: {media.id}, Type: {media.media_type}, Likes: {media.like_count}")



if __name__ == '__main__':
    main()