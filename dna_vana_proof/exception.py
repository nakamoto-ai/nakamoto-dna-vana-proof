import traceback
import sys


def raise_custom_exception(
    error_type="Unknown", message="", status_code="N/A", response="N/A", context_lines=10, **kwargs
):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    if exc_traceback:
        calling_traceback = traceback.format_exc()
    else:
        stack = traceback.extract_stack()[:-1]
        calling_traceback = "".join(traceback.format_list(stack[-context_lines:]))

    important_info = {
        "error_type": error_type,
        "message": message,
        "response": response,
        "status_code": status_code,
    }

    for k, v in kwargs.items():
        important_info[k] = v

    formatted_important_info = "\n".join([f"{{{{ {key}: {value} }}}}" for key, value in important_info.items()])

    combined_logs = f"{formatted_important_info}\n{calling_traceback}"

    raise Exception(combined_logs)
