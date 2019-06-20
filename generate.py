from time import sleep
orig_file = open('testing.txt', 'r')
lines = orig_file.readlines()
i = 0
while True:
    for line in lines:
        new_text = line
        new_file = open('data_'+str(i), 'w')
        new_file.write(new_text)
        new_file.close()
        print(i)
        i += 1
        sleep(2)
orig_file.close()
