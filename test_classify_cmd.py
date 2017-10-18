import subprocess
import fire


def main(num=8):
	for i in range(8):
		subprocess.Popen(['python','test_classify.py'])

if __name__ == '__main__':
	fire.Fire(main)