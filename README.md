Files: 

* configureMaster.sh
	- This file configures the master node in our spark job; it ensures that Stringtie and SAMtools are both available for usage in the /usr/bin directory and it indexes the data file (given as argument) for easy random access. 
* fullSparkSubmitRun.py
	- The main run file for our project.
* gtf_merge.py
	- Contains code for merging the GTF results given by each individual Stringtie run, normalizing FPKM and TPM. 
* load_balancing.py
	- Contains code for running the load-balancing algorithm detailed in our report. 
* machine.py 
	- Machine object class for use in load balancing (we assign jobs to machines). 
* overlapParser.py
	- Creates the groups of overlapping or neighboring reads given a BAM file (this determines the structure of our initial graph. 
* stringtie_mod
	- The script to which we pipe each partition; essentially, this does the Stringtie run. 
* timeRun.sh
	- Times a run of our pipeline. 

In order to run this pipeline, you will need an installation of PySAM, Spark (v 1.5.0) and GraphFrame (0.2.0-spark1.5-s_2.10). PySAM is installable via pip. Other versions of Spark and GraphFrame should work but are not fully tested.
