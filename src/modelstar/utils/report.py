import webbrowser


def view_in_browser(file_path: str):
    local_url = 'file://' + file_path
    webbrowser.open(local_url, new=2)
