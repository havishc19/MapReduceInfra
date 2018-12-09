import sys
import os

def count(filename, word_dict):
	with open(filename, "r") as f:
		for line in f:
			words = line.split()
			for word in words:
				if word[-1] == '.':
					word = word[:-1]
				if word not in word_dict:
					word_dict[word] = 1
				else:
					word_dict[word] += 1

if len(sys.argv) != 2:
	print("usage: python count.py <config_file>")
	exit(-1)

config = sys.argv[1]
input_files = ""
output_dir = ""
try:
	with open(config, "r") as f:
		for line in f:
			tokens = line.split('=')
			if tokens[0] == "input_files":
				input_files = tokens[1].rstrip().split(',')
			elif tokens[0] == "output_dir":
				output_dir = tokens[1].rstrip()
except Exception:
	print("Error in config")
	exit(-1)

i_words = {}
for f_name in input_files:
	count(f_name, i_words)


o_words = {}
output_files = os.listdir(output_dir)
for f_name in output_files:
	if '.txt' in f_name:
		with open(os.path.join(output_dir,f_name),'r') as f:
			for line in f:
				tokens = line.split(',')
				o_words[tokens[0]] = tokens[1]

all_correct = True
if len(o_words) != len(i_words):
	all_correct = False
	print("Key space length mismatch")
	if len(o_words) < len(i_words):
		print("Missing output words:")
		for key in i_words:
			if key not in o_words:
				print(key)
	else:
		print("Excess output words:")
		for key in o_words:
			if key not in i_words:
				print(key)

for key in o_words:
	if key in i_words:
		if int(i_words[key]) != int(o_words[key]):
			print("[KEY:{}] | [INPUT:{}] | [OUTPUT:{}].".format(key, i_words[key], o_words[key]))

if not all_correct:
	print("FAILED")
else:
	print("SUCCESS! All correct")
