

#read libsvm file 
def read_libsvm_file(filename):
    new_file = open(filename + "_redo", 'w')
    with open(filename, 'r') as f:

        for line in f.readlines():
            readed_line = line.strip().split(' ')
            label,current_line =  readed_line[0],readed_line[1:]

            new_file.write(label + " " + " ".join([str(int(x.split(':')[0]) + 1) + ":" +  x.split(':')[1] for x in current_line]) + "\n")



read_libsvm_file("./utility2.txt")