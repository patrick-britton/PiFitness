

def reverse_key_lookup(d, value):
    # Returns the key value for a dictionary when passed a list item
    matches = [k for k, v in d.items() if v == value]
    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0]
    else:
        return matches