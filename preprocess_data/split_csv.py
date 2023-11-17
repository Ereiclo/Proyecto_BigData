def split_csv(line):
    line = line.strip()
    elems = []
    current_elem = ""
    in_quotes = False
    for char in line:
        if char == "," and not in_quotes:
            elems.append(current_elem)

            current_elem = ""
            continue
        elif char == '"':
            in_quotes = not in_quotes

        current_elem += char

    elems.append(current_elem)

    return elems
