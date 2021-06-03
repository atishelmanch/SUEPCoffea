import os
import re
import pickle 

from coffea.processor import run_uproot_job, futures_executor

from python.SUEP_Producer import *
from python.SumWeights import *

import uproot3 as uproot
import argparse

from matplotlib.colors import LogNorm

import time

# singularity shell -B ${PWD} /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest
# python3 condor_SUEP_WS.py --isMC=0 --era=2018 --infile=ETT_test_197.root --treename="tuplizer/ETTAnalyzerTree"

parser = argparse.ArgumentParser("")
parser.add_argument('--isMC', type=int, default=1, help="")
parser.add_argument('--jobNum', type=int, default=1, help="")
parser.add_argument('--era', type=str, default="2018", help="")
parser.add_argument('--doSyst', type=int, default=1, help="")
parser.add_argument('--infile', type=str, default=None, help="")
parser.add_argument('--dataset', type=str, default="X", help="")
parser.add_argument('--nevt', type=str, default=-1, help="")
parser.add_argument('--treename', type=str, default="Events", help="")
parser.add_argument('--inDir', type=str, default="", help="")

options = parser.parse_args()


def inputfile(nanofile):
    tested = False
    forceaaa = False
    pfn = os.popen("edmFileUtil -d %s" % (nanofile)).read()
    pfn = re.sub("\n", "", pfn)
    print((nanofile, " -> ", pfn))
    if (os.getenv("GLIDECLIENT_Group", "") != "overflow" and
            os.getenv("GLIDECLIENT_Group", "") != "overflow_conservative" and not
            forceaaa):
        if not tested:
            print("Testing file open")
            testfile = uproot.open(pfn)
            if testfile:
                print("Test OK")
                nanofile = pfn
            else:
                if "root://cms-xrd-global.cern.ch/" not in nanofile:
                    nanofile = "root://cms-xrd-global.cern.ch/" + nanofile
                forceaaa = True
        else:
            nanofile = pfn
    else:
        if "root://cms-xrd-global.cern.ch/" not in nanofile:
            nanofile = "root://cms-xrd-global.cern.ch/" + nanofile
    return nanofile


options.dataset='QCD'

pre_selection = ""

if float(options.nevt) > 0:
    print((" passing this cut and : ", options.nevt))
    pre_selection += ' && (Entry$ < {})'.format(options.nevt)

pro_syst = []
ext_syst = []

modules_era = []

modules_era.append(SUEP_NTuple(isMC=options.isMC, era=int(options.era), do_syst=1, syst_var='', sample=options.dataset,
                         haddFileName="tree_%s.root" % str(options.jobNum)))
if options.isMC and options.doSyst==1:
   for sys in pro_syst:
       for var in ["Up", "Down"]:
           modules_era.append(SUEP_NTuple(options.isMC, str(options.era), do_syst=1,
                                    syst_var=sys + var, sample=options.dataset,
                                    haddFileName=f"tree_{options.jobNum}_{sys}{var}.root"))
   
   for sys in ext_syst:
       for var in ["Up", "Down"]:
           modules_era.append(
               SUEP_NTuple(
                   options.isMC, str(options.era),
                   do_syst=1, syst_var=sys + var,
                   weight_syst=True,
                   sample=options.dataset,
                   haddFileName=f"tree_{options.jobNum}_{sys}{var}.root",
               )
           )

for i in modules_era:
    print("modules : ", i)

# files = ["%s/%s"%(options.inDir,f) for f in os.listdir(options.inDir) if f.endswith(".root")]

# print("Selection : ", pre_selection)
tstart = time.time()
f = uproot.recreate("tree_%s_WS.root" % str(options.jobNum))
for instance in modules_era:
    output = run_uproot_job(
        # {instance.sample: files},
        {instance.sample: [options.infile]},
        # treename='Events',
        treename=options.treename,
        processor_instance=instance,
        executor=futures_executor,
        executor_args={'workers': 10},
        chunksize=500000
    )

    ##-- https://coffeateam.github.io/coffea/notebooks/histograms.html
    ##-- http://github.com/CoffeaTeam/coffea/blob/515877fa55e914ef82f51f686d1a0eaa9f0f71d1/coffea/hist/hist_tools.py#L903
    for h, hist in output.items():

        xaxis = hist.axes()[0]
        histName = "%s_2d"%(h)
        ax = plot2d(
            hist, 
            xaxis = xaxis,
            # patch_opts 
            patch_opts = dict(
                vmin = 1,
                cmap = 'jet',
                norm = LogNorm()
                )
            )

        ax.set_ylim(0, 256)
        ax.set_xlim(0, 256)
        ax.set_yscale('log')
        ax.set_xscale('log')

        fig = ax.figure

        ol = "/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias/"

        ##-- Pickle the axis for post-processing changes 
        pickle.dump( ax, open( '%s/%s.p'%(ol, histName), "wb" ))

        ##-- Save output plots         
        fig.savefig('%s/%s.png'%(ol, histName))
        fig.savefig('%s/%s.pdf'%(ol, histName))

        print("Saved plot %s/%s.png"%(ol, histName))
        print("Saved plot %s/%s.pdf"%(ol, histName))

        ##-- To open 
        # X_train_EB_reshape_loaded = pickle.load( open( "X_train_EB_reshape_pt%s.p"%(sample), "rb" ) )
        
        # ax.figure.savefig('plot.png')
        # ax.figure.savefig('plot.pdf')
        # print("h:",h)
        # print("hist:",hist)        
        # f[h] = export1d(hist) ##-- for 1d histogram exporting to output root file 
        # f[h] = plot2d(hist, xaxis = 'time_QCD_sevzero_all')
        # f[h] = plot2d(hist)
        # print(f'wrote {h} to tree_{options.jobNum}_WS.root')

# modules_gensum = []

# modules_gensum.append(GenSumWeight(isMC=options.isMC, era=int(options.era), do_syst=1, syst_var='', sample=options.dataset,
#                          haddFileName="tree_%s.root" % str(options.jobNum)))

# for instance in modules_gensum:
#     output = run_uproot_job(
#         # {instance.sample: files},
#         {instance.sample: [options.infile]},
#         # treename='Runs',
#         treename=options.treename,
#         processor_instance=instance,
#         executor=futures_executor,
#         executor_args={'workers': 10},
#         chunksize=500000
#     )
#     for h, hist in output.items():
#         ax = plot2d(hist, xaxis = 'time_QCD_sevzero_all')
#         ax.figure.savefig('plot.png')
#         ax.figure.savefig('plot.pdf')        
#         # print("h:",h)
#         # print("hist:",hist)
#         # f[h] = plot2d(hist, xaxis = 'time_QCD_sevzero_all')
#         # f[h] = export1d(hist) ##-- for 1d histogram exporting to output root file 
#         # print(f'wrote {h} to tree_{options.jobNum}_WS.root')
