import webbrowser
from modelstar import logger


def view_in_browser(file_path: str):
    
    logger.echo('Opening the report in a new tab in your browser.')

    local_url = 'file://' + file_path
    webbrowser.open(local_url, new=2)
