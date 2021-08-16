# Example steps 

```
  git clone git@github.com:atishelmanch/ETT_Coffea.git
  cd SUEPCoffea
  singularity shell -B ${PWD} -B /afs -B /eos /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest ##-- Mount /afs and /eos space to run on files in those locations. https://hsf-training.github.io/hsf-training-docker/10-singularity/index.html
  python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/cms/store/group/dpg_ecal/alca_ecalcalib/Trigger/DoubleWeights/Run_324725_Run2018D_ZeroBias_FullReadout/ETTAnalyzer_CMSSW_11_3_0_StripZeroing_OfflineWeights/210816_144325/0000/" --treename="ETTAnalyzerTree" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/FullReadout_singleFile/" --condor="0"
```
