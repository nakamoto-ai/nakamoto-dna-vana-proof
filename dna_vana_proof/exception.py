import traceback
import sys


def raise_custom_exception(error_type="Unknown", message="", status_code="N/A", response="N/A"):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    formatted_traceback = "".join(traceback.format_stack())

    important_info = {"error_type": error_type, "message": message, "response": response, "status_code": status_code}

    formatted_important_info = "\n".join([f"{{{{ {key}: {value} }}}}" for key, value in important_info.items()])

    combined_logs = f"{formatted_important_info}\n{formatted_traceback}"

    raise Exception(combined_logs)
