# ETT Coffea

This is a repository originating from [SUEPPhysics](https://github.com/SUEPPhysics/SUEPCoffea), whose purpose is to process CMS ECAL trigger team analyzer output files. 

## Setup 

```
  git clone git@github.com:atishelmanch/ETT_Coffea.git
  cd ETT_Coffea  
```

## Example to run locally   

```
  singularity shell -B ${PWD} -B /afs -B /eos /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest ##-- Mount /afs and /eos space to run on files in those locations. https://hsf-training.github.io/hsf-training-docker/10-singularity/index.html
  python3 RunProducer.py --inDir="/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/Run_324725_Run2018D_ZeroBias_FullReadout/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_OfflineWeights/210816_144325/0000/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/FullReadout_singleFile/" --condor="0"
```

## Example to run over Condor  

```
  python RunProducer_Condor.py --tag=210809_140837
```
