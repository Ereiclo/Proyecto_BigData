data = open("recos_supermercados/supermercados.csv", "r")
new_file = open("recos_supermercados/supermercados_clean.csv", "w")
i = 0


def split_csv(line):
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


for line in data:

    line = line.split(",", 1)[1]
    new_line = ""

    index = 0

    elems = split_csv(line)

    for elem in elems:
        # trim elem and add to new_line
        cleaned_elem = elem.strip()
        if index == 5 and i > 0 and cleaned_elem != "":
            try:
                cleaned_elem = str(int(float(cleaned_elem)))
            except:
                print(line)
                raise 5

        new_line += cleaned_elem + ","
        index += 1
    new_line = new_line[:-1]

    new_file.write(new_line + "\n")
    i += 1
