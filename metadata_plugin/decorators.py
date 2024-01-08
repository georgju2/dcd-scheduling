from functools import wraps

ANNOTATION_KEY = "ANNOTATION_KEY"

def annotate(**metadata):
    def task_decorator(task_func):
        @wraps(task_func)
        def wrapper(*args, **kwargs):
            return task_func(*args, **kwargs)
        
        if not hasattr(wrapper, ANNOTATION_KEY):
            setattr(wrapper, ANNOTATION_KEY, {})
        
        for key, value in metadata.items():
            wrapper.ANNOTATION_KEY[key] = value
        
        return wrapper
    return task_decorator

