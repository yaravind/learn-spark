# Visit the project site https://yaravind.github.io/learn-spark/

## Running notebooks

```
➜  learn-spark git:(master) ✗ git clone https://github.com/yaravind/learn-spark.git

➜  learn-spark git:(master) ✗ cd learn-spark

➜  learn-spark git:(master) ✗ pwd
/Users/aravind/Documents/WS/learn-spark

➜  learn-spark git:(master) ✗ ltr
total 32
-rw-r--r--   1 o60774  NA\Domain Users   1.0K Apr  4 13:49 LICENSE
drwxr-xr-x   7 o60774  NA\Domain Users   224B Apr  4 13:49 stackoverflow/
drwxr-xr-x   7 o60774  NA\Domain Users   224B Apr  4 13:49 timeusage/
drwxr-xr-x   7 o60774  NA\Domain Users   224B Apr  4 13:49 wikipedia/
drwxr-xr-x   4 o60774  NA\Domain Users   128B May 30 10:20 data/
drwxr-xr-x  14 o60774  NA\Domain Users   448B May 30 14:23 docs/
-rw-r--r--@  1 o60774  NA\Domain Users    65B May 30 14:24 README.md
drwxr-xr-x  15 o60774  NA\Domain Users   480B May 30 14:27 stackoverflow-complete/
-rw-r--r--   1 o60774  NA\Domain Users   208B May 30 14:36 run.sh
-rw-r--r--   1 o60774  NA\Domain Users   174B May 30 14:37 run.bat
drwxr-xr-x   5 o60774  NA\Domain Users   160B May 30 14:41 notebooks/

➜  learn-spark git:(master) ✗ ./run.sh
zsh: permission denied: ./run.sh

➜  learn-spark git:(master) ✗ chmod 755 run.sh

➜  learn-spark git:(master) ✗ ./run.sh
Executing the command: jupyter notebook
[I 18:47:07.060 NotebookApp] Writing notebook server cookie secret to /home/jovyan/.local/share/jupyter/runtime/notebook_cookie_secret
[I 18:47:07.462 NotebookApp] Loading IPython parallel extension
[I 18:47:08.246 NotebookApp] JupyterLab extension loaded from /opt/conda/lib/python3.7/site-packages/jupyterlab
[I 18:47:08.246 NotebookApp] JupyterLab application directory is /opt/conda/share/jupyter/lab
[I 18:47:08.250 NotebookApp] Serving notebooks from local directory: /home/jovyan
[I 18:47:08.250 NotebookApp] The Jupyter Notebook is running at:
[I 18:47:08.250 NotebookApp] http://d15e303fe663:8888/?token=4d02b0bb3e475c544bc4005b7edeb37409af45b76635d236
[I 18:47:08.250 NotebookApp]  or http://127.0.0.1:8888/?token=4d02b0bb3e475c544bc4005b7edeb37409af45b76635d236
[I 18:47:08.250 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 18:47:08.255 NotebookApp]

    To access the notebook, open this file in a browser:
        file:///home/jovyan/.local/share/jupyter/runtime/nbserver-6-open.html
    Or copy and paste one of these URLs:
        http://xsdfssddfw:8888/?token=4d02b0bb3e475c544bc4005b7edeb37409af45b76635d236
     or http://127.0.0.1:8888/?token=4d02b0bb3e475c544bc4005b7edeb37409af45b76635d236
```
Command+Click on the http://127... url