
# before running, conda activate pyinstaller 
#

all: fixTree 

fixTree:
	rm -rf dist/fixTree
	pyinstaller fixTree.py
	sudo chown root:root dist/fixTree/fixTree
	sudo chmod ug+s dist/fixTree/fixTree
	sudo rsync -av dist/fixTree ~rdb9/sbin/

