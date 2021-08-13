# Example steps 

```
  git clone git@github.com:atishelmanch/SUEPCoffea.git -b bbZZ 
  cd SUEPCoffea
  singularity shell -B ${PWD} -B /afs -B /eos /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest ##-- Mount /afs and /eos space to run on files in those locations. https://hsf-training.github.io/hsf-training-docker/10-singularity/index.html
  python3 condor_SUEP_WS.py --era=2018 --inDir="/eos/user/a/atishelm/ntuples/Test_Coffea/" --treename="Events" --outDir="/eos/user/a/atishelm/www/EcalL1Optimization/ZeroBias_singleFile/" --condor="0"
```
