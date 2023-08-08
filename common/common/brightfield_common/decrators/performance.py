import time

def timing(*args, **kwargs):
    def method(actual_function):
        def wrapper(*args, **kwargs):
            start = time.time()
            name = actual_function.__name__
            response = actual_function(*args, **kwargs)
            print(f'{name} took {round(time.time() - start)} seconds:')
            return response
        return wrapper
    return method