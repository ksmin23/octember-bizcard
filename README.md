# octember

 bizcard managment app


### how-to make layer package in python 

```
$ source bin/activate
$ mkdir -p python_modules
$ pip install -r requirements.txt -t python_modules
$ mv python_modules/ python
$ zip -r octember-es-lib.zip python/
$ aws s3 cp octember-es-lib.zip s3://octember-use1/var/
```

