##-- example command: python run_SUEPProducer.py --era=2018 --tag=FewFilesForTesting2

import os, sys
import argparse
import logging
import pwd
import subprocess
import shutil
import time
import glob

#!/usr/bin/python

logging.basicConfig(level=logging.DEBUG)

##-- script_TEMPLATE = """#!/bin/bash
# script_TEMPLATE = """#!/bin/sh -e

# #!/usr/bin/env bash

script_TEMPLATE = """#!/bin/bash
##source /cvmfs/cms.cern.ch/cmsset_default.sh

##source /cvmfs/cms.cern.ch/cmsset_default.sh
#source /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest
export SCRAM_ARCH=slc7_amd64_gcc820

# echo "ls /eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights :"
# ls /eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights

export LD_LIBRARY_PATH_STORED=$LD_LIBRARY_PATH_STORED

echo "ls /eos :"
ls /eos
echo "checked /eos"


# cd {cmssw_base}/src/SUEPCoffea/
#cd {cmssw_base}/src/
# eval `scramv1 runtime -sh`
#eval `scramv1 ru -sh`
echo
echo $_CONDOR_SCRATCH_DIR
cd   $_CONDOR_SCRATCH_DIR
echo
echo "... start job at" `date "+%Y-%m-%d %H:%M:%S"`
echo "----- directory before running:"
ls -lR .
echo "----- CMSSW BASE, python path, pwd:"
echo "+ CMSSW_BASE  = $CMSSW_BASE"
echo "+ PYTHON_PATH = $PYTHON_PATH"
echo "+ PWD         = $PWD"

python condor_SUEP_WS.py --jobNum=$1 --era={era} --infile=$2 --treename="ETTAnalyzerTree"
mv yields.p 
mv values.p 

#cp $2 temp_$1.root
#python condor_SUEP_WS.py --jobNum=$1 --isMC=(((ismc))) --era={era} --infile=temp_$1.root

#rm temp_$1.root
echo "----- transfer output to eos :"
mv tree_$1. {outputdir}
echo "----- directory after running :"
ls -lR .
echo " ------ THE END (everyone dies !) ----- "
"""

condor_TEMPLATE = """
request_disk          = 1024
executable            = {jobdir}/script.sh
arguments             = $(ProcId) $(jobid) 
# transfer_input_files  = {transfer_files}
#transfer_input_files  = {transfer_files}, $Fp(/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018_EBOnly/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_EBOnly/210626_062710/FewFilesForTesting/ActualFiles/)
transfer_input_files = {transfer_files}, $(jobid)

#environment = "LD_LIBRARY_PATH_STORED=/usr/bin:/usr/local/bin:/usr/foo"
environment = "LD_LIBRARY_PATH_STORED=/eos" 

output                = $(ClusterId).$(ProcId).out
error                 = $(ClusterId).$(ProcId).err
log                   = $(ClusterId).$(ProcId).log
initialdir            = {jobdir}
#environment           = 'vanilla'
# transfer_output_files = yields.p,values.p 
transfer_output_remaps = "yields.p={output_dir}/yields_$(ProcId).p;values.p={output_dir}/values_$(ProcId).p"
#Requirements = HasSingularity
+JobFlavour           = "{queue}"
#+SingularityImage = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest"

HasDocker = true
universe = docker
docker_image = coffeateam/coffea-dask:latest

queue jobid from {jobdir}/inputfiles.dat
"""

def main():
    parser = argparse.ArgumentParser(description='Famous Submitter')
    parser.add_argument("-t"   , "--tag"   , type=str, default="Exorcism"  , help="production tag", required=True)
    # parser.add_argument("-isMC", "--isMC"  , type=int, default=1          , help="")
    parser.add_argument("-q"   , "--queue" , type=str, default="espresso", help="")
    parser.add_argument("-e"   , "--era"   , type=str, default="2017"     , help="")
    parser.add_argument("-f"   , "--force" , action="store_true"          , help="recreate files and jobs")
    parser.add_argument("-s"   , "--submit", action="store_true"          , help="submit only")
    parser.add_argument("-dry" , "--dryrun", action="store_true"          , help="running without submission")

    options = parser.parse_args()

    cmssw_base = os.environ['CMSSW_BASE']
    # indir = "/mnt/hadoop/scratch/freerc/SUEP/{}/".format(options.tag)
    ##-- After it's working, replace final direc with: 0000, 0001, 0002, 0003 
    # indir = "/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018_EBOnly/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_EBOnly/210626_062710/{}/".format(options.tag)
    indir = "/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/ZeroBias_2018_EBOnly/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_EBOnly/210626_062710/{}/".format(options.tag)

    pattern = "WZ"
    for sample in os.listdir(indir):
        if "merged" in sample:
            continue
        if "WS" in sample:
            continue
        #if pattern not in sample:
        #    continue

        jobs_dir = '_'.join(['jobs', options.tag, sample])
        logging.info("-- sample_name : " + sample)

        if os.path.isdir(jobs_dir):
            if not options.force:
                logging.error(" " + jobs_dir + " already exist !")
                continue
            else:
                logging.warning(" " + jobs_dir + " already exists, forcing its deletion!")
                shutil.rmtree(jobs_dir)
                #os.system("rm -rf {}".format(jobs_dir))
                os.mkdir(jobs_dir)
        else:
            os.mkdir(jobs_dir)
        with open(os.path.join(jobs_dir, "inputfiles.dat"), 'w') as infiles:
            in_files = glob.glob("{indir}/{sample}/*.root".format(sample=sample, indir=indir))
            # print("in_files:",in_files)
            for name in in_files:
                infiles.write(name+"\n")
            infiles.close()
        outdir = indir + sample + "_output/"
        os.system("mkdir -p {}".format(outdir))

        with open(os.path.join(jobs_dir, "script.sh"), "w") as scriptfile:
            script = script_TEMPLATE.format(
                cmssw_base=cmssw_base,
                # ismc=options.isMC,
                era=options.era,
                outputdir=outdir,
                # indir = "{indir}/{sample}/".format(sample=sample, indir=indir),
            )
            scriptfile.write(script)
            scriptfile.close()

        with open(os.path.join(jobs_dir, "condor.sub"), "w") as condorfile:
            allFiles = [
                "../condor_SUEP_WS.py",
                "../python/SUEP_Producer.py",
                "../python/SumWeights.py"
            ]
            # in_files = glob.glob("{indir}/{sample}/*.root".format(sample=sample, indir=indir))
            # for fi in in_files:
                # allFiles.append(fi)
            # print("allFiles:",allFiles)
            condor = condor_TEMPLATE.format(
                transfer_files = ",".join(allFiles),
                output_dir = outdir,
                # transfer_files= ",".join([
                    # "../condor_SUEP_WS.py",
                    # "../python/SUEP_Producer.py",
                    # "../python/SumWeights.py",
                    # inputFile
                # ]),
                jobdir=jobs_dir,
                queue=options.queue
            )
            condorfile.write(condor)
            condorfile.close()
        if options.dryrun:
            continue

        htc = subprocess.Popen(
            "condor_submit " + os.path.join(jobs_dir, "condor.sub"),
            shell  = True,
            stdin  = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            close_fds=True
        )
        out, err = htc.communicate()
        exit_status = htc.returncode
        logging.info("condor submission status : {}".format(exit_status))

if __name__ == "__main__":
    main()
    print("DONE")
