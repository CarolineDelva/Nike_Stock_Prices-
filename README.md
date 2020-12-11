# Pset 1

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Before you begin...](#before-you-begin)
  - [Document code and read documentation](#document-code-and-read-documentation)
  - [Docker shortcut](#docker-shortcut)
  - [Pipenv](#pipenv)
    - [Installation](#installation)
    - [Usage](#usage)
      - [Debugging pipenv installs](#debugging-pipenv-installs)
      - [Pipenv inside docker](#pipenv-inside-docker)
  - [Credentials and data](#credentials-and-data)
    - [Using `awscli`](#using-awscli)
      - [Installation (via pipenv)](#installation-via-pipenv)
    - [Configure `awscli`](#configure-awscli)
      - [Make a .env (pronounced "dotenv") file](#make-a-env-pronounced-dotenv-file)
      - [Other methods](#other-methods)
    - [Copy the data locally](#copy-the-data-locally)
    - [Set the Travis environment variables](#set-the-travis-environment-variables)
- [Problems](#problems)
  - [Hashed strings](#hashed-strings)
    - [Implement a standardized string hash](#implement-a-standardized-string-hash)
    - [True salting](#true-salting)
  - [Atomic writes](#atomic-writes)
    - [Implement an atomic write](#implement-an-atomic-write)
  - [Parquet](#parquet)
  - [Your main script](#your-main-script)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Before you begin...

**Add your Travis and Code Climate badges** to the
top of this README, using the markdown template for your master branch.

### Document code and read documentation

For some problems we have provided starter code. Please look carefully at the
doc strings and follow all input and output specifications.

For other problems, we might ask you to create new functions, please document
them using doc strings! Documentation factors into the "python quality" portion
of your grade.

### Docker shortcut

See [drun_app](./drun_app):

```bash
docker-compose build
./drun_app python # Equivalent to docker-compose run app python
```

### Pipenv

This pset will require dependencies.  Rather than using a requirements.txt, we
will use [pipenv](https://pipenv.readthedocs.io/en/latest/) to give us a pure,
repeatable, application environment.

#### Installation

If you are using the Docker environment, you should be good to go.  Mac/windows
users should [install
pipenv](https://pipenv.readthedocs.io/en/latest/#install-pipenv-today) into
their main python environment as instructed.  If you need a new python 3.7
environment, you can use a base
[conda](https://docs.conda.io/en/latest/miniconda.html) installation.

```bash
# Optionally create a new base python 3.7
conda create -n py37 python=3.7
conda activate py37
pip install pipenv
pipenv install ...
```

```bash
pipenv install --dev
pipenv run python some_python_file
```

If you get a TypeError, see [this
issue](https://github.com/pypa/pipenv/issues/3363)

#### Usage

Rather than `python some_file.py`, you should run `pipenv run python
some_file.py` or `pipenv shell` etc

***Never*** pip install something!  Instead you should `pipenv install pandas`
or `pipenv install --dev ipython`.  Use `--dev` if your app only needs the
dependency for development, not to actually do it's job.

Pycharm [works great with
pipenv](https://www.jetbrains.com/help/pycharm/pipenv.html)

Be sure to commit any changes to your [Pipfile](./Pipfile) and
[Pipfile.lock](./Pipfile.lock)!

##### Debugging pipenv installs

If a pipenv install fails, it can be hard to see what the root cause is.  Try
these tips:

1. Ensure you don't have a bad package in your Pipfile (pipenv sometimes adds
them even if the install fails).  You can manually delete lines for packages you
don't want.

2. Try verbose mode, with `pipenv install -v ...`.

3. Try manually installing with `pip install ...` or `pip install -v ...`. This
will often show you the 'real' error.  If it works, don't forget to `pipenv
install ...` to ensure the dep is tracked appropriately

##### Pipenv inside docker

Because of the way docker freezes the operating system, installing a new package
within docker is a two-step process:

```bash
docker-compose build

# Now i want a new thing
./drun_app pipenv install pandas # Updates pipfile, but does not rebuild image
# running ./drun_app python -c "import pandas" will fail!

# Rebuild
docker-compose build
./drun_app python -c "import pandas" # Now this works
```

### Credentials and data

Git should be for code, not data, so we've created an S3 bucket for problem set
file distribution.  For this problem set, we've uploaded a data set of your
answers to the "experience demographics" quiz that you should have completed in
the first week. In order to access the data in S3, we need to install and
configure `awscli` both for running the code locally and running our tests in
Travis.

You should have created an IAM key in your AWS account.  DO NOT SHARE THE SECRET
KEY WITH ANYONE. It gives anyone access to the S3 bucket.  It must not be
committed to your code.

For more reference on security, see [Travis Best
Practices](https://docs.travis-ci.com/user/best-practices-security/#recommendations-on-how-to-avoid-leaking-secrets-to-build-logs)
and [Removing Sensitive
Data](https://help.github.com/articles/removing-sensitive-data-from-a-repository/).

#### Using `awscli`

AWS provides a [CLI
tool](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
that helps interact with the many different services they offer.

##### Installation (via pipenv)

We have already installed `awscli` into your pipenv.  It is available within the
pipenv shell and the docker container via the same mechanism.

```bash
pipenv run aws --help
./drun_app aws --help
```

#### Configure `awscli`

Now configure `awscli` to access the S3 bucket.

##### Make a .env (pronounced "dotenv") file

Create a [.env](.env) file that looks something like this:

```
AWS_ACCESS_KEY_ID=XXXX
AWS_SECRET_ACCESS_KEY=XXX
```

The `.env` file should be in your root project directory, the same directory as
this README.

***DO NOT*** commit this file to the repo (it's already in your
[.gitignore](.gitignore))

Both docker and pipenv will automatically inject these variables into your
environment!  Whenever you need new env variables, add them to a dotenv. You
***DO NOT*** need to install any additional modules (notably `python-dotenv`) in
order for this to work.

See env refs for
[docker](https://docs.docker.com/compose/environment-variables/) and
[pipenv](https://pipenv.readthedocs.io/en/latest/advanced/#automatic-loading-of-env)
for more details.

##### Other methods

According to the
[documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
the easiest way is:

```bash
aws configure
AWS Access Key ID [None]: ACCESS_KEY
AWS Secret Access Key [None]: SECRET_KEY
Default region name [None]:
Default output format [None]:
```

There are other, more complicated, configurations outlined in the documentation.
Feel free to use a solution using environment variables, a credentials file, a
profile, etc.

#### Copy the data locally

Read the [Makefile](Makefile) and [.travis.yml](./.travis.yml) to see how to
copy the data locally.

Note that we are using a [Requestor
Pays](https://docs.aws.amazon.com/AmazonS3/latest/dev/RequesterPaysBuckets.html)
bucket.  You are responsible for usage charges.

You should now have a new folder called `data` in your root directory with the
data we'll use for this problem set. You can find more details breaking down
this command at the [S3 Documentation
Site](https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html).

#### Set the Travis environment variables

This is Advanced Data Science, so of course we also want to automate our tests
and pset using CI/CD.  Unfortunately, we can't upload our .env or run  `aws
configure` and interactively enter the credentials for the Travis builds, so we
have to configure Travis to use the access credentials without compromising the
credentials in our git repo.

We've provided a working `.travis.yml` configuration that only requires the AWS
credentials when running on the master branch, but you will still need to the
final step of adding the variables for your specific pset repository.

To add environment variables to your Travis environment, you can use of the
following options:

* Navigating to the settings, eg https://travis-ci.com/csci-e-29/YOUR_PSET_REPO/settings
* The [Travis CLI](https://github.com/travis-ci/travis.rb)
* encrypting into the `.travis.yml` as instructed [here](https://docs.travis-ci.com/user/environment-variables/#defining-encrypted-variables-in-travisyml).

Preferably, you should only make your 'prod' credentials available on your
master branch: [Travis
Settings](https://docs.travis-ci.com/user/environment-variables/#defining-variables-in-repository-settings)

You can chose the method you think is most appropriate.  Our only requirement is
that ***THE KEYS SHOULD NOT BE COMMITTED TO YOUR REPO IN PLAIN TEXT ANYWHERE***.

For more information, check out the [Travis Documentation on Environment
Variables](https://docs.travis-ci.com/user/environment-variables/)

__*IMPORTANT*__: If you find yourself getting stuck or running into issues,
please post on Piazza and ask for help.  We've provided most of the instructions
necessary for this step and do not want you spinning your wheels too long just
trying to download the data.

## Problems

### Hashed strings

It can be extremely useful to
[hash](https://en.wikipedia.org/wiki/Cryptographic_hash_function) a string or
other data for various reasons - to distribute/partition it, to anonymize it, or
otherwise conceal the content.

#### Implement a standardized string hash

Use `sha256` as the backbone algorithm from
[hashlib](https://docs.python.org/3/library/hashlib.html). Hashlib is part of
the Python standard library, so you do not have to install it in order to import
it into your modules.

A `salt` is a prefix that may be added to increase the randomness or otherwise
change the outcome.  It may be a `str` or `bytes` string, or empty.

Implement it in [hash_str.py](pset_1/hash_str.py), where the return value is the
`.digest()` of the hash, as a `bytes` array:

```python
def hash_str(some_val: Union[str, bytes], salt: Union[str, bytes] = "") -> bytes:
    """Converts strings to hash digest

    See: https://en.wikipedia.org/wiki/Salt_(cryptography)

    :param some_val: thing to hash, can be str or bytes
    :param salt: Add randomness to the hashing, can be str or bytes
    :return: sha256 hash digest of some_val with salt, type bytes
    """
```

Note you will need to
[encode](https://docs.python.org/3/library/stdtypes.html#str.encode) string
values into bytes (use `.encode()`!).

As an example, `hash_str('world!', salt='hello, ').hex()[:6] == '68e656'`

Note that if we ever ask you for a bytes value in Canvas, the expectation is the
hexadecimal representation as illustrated above.

#### True salting

Note, however, that hashing isn't very secure without a secure salt.  We can
take raw `bytes` to get something with more entropy than standard text provides.

Let's designate an environment variable, `CSCI_SALT`, which will contain
hex-encoded bytes.  Implement the function `pset_1.hash_str.get_csci_salt`
which pulls and decodes an environment variable.  In Canvas, you will be given a
random salt taken from [random.org](http://random.org) for real security.

### Atomic writes

Use the module `pset_1.io`.  We will implement an atomic writer.

Atomic writes are used to ensure we never have an incomplete file output.
Basically, they perform the operations:

1. Create a temporary file which is unique (possibly involving a random file
   name)
2. Allow the code to take its sweet time writing to the file
3. Rename the file to the target destination name.

If the target and temporary file are on the same filesystem, the rename
operation is ***atomic*** - that is, it can only completely succeed or fail
entirely, and you can never be left with a bad file state.

See notes in
[Luigi](https://luigi.readthedocs.io/en/stable/luigi_patterns.html#atomic-writes-problem)
and the [Thanksgiving
Bug](https://www.arashrouhani.com/luigi-budapest-bi-oct-2015/#/21)

#### Implement an atomic write

Start with the following in [io.py](./pset_1/io.py):

```python
@contextmanager
def atomic_write(file: Union[str, os.PathLike], mode: str="w", as_file: bool=True, **kwargs) -> ContextManager:
    """Write a file atomically

    :param file: str or :class:`os.PathLike` target to write

    :param bool as_file:  if True, the yielded object is a :class:File.
        (eg, what you get with `open(...)`).  Otherwise, it will be the
        temporary file path string

    :param kwargs: anything else needed to open the file

    :raises: FileExistsError if target exists

    Example::

        with atomic_write("hello.txt") as f:
            f.write("world!")

    """
    ...
```

 Key considerations:

 * You can use [tempfile](https://docs.python.org/3.6/library/tempfile.html),
   write to the same directory as the target, or both.
   What are the tradeoffs? Add code comments for anything critical
 * Ensure the file does not already exist before you begin writing
 * Ensure the file is deleted if the writing code fails
 * Ensure the temporary file has the same extension(s) as the target.  This is
   important for any code that may infer something from the path (for example,
   extensions can have multiple periods such as `.tar.gz`).
 * If the writing code fails and you try again, the temp file should be new -
   you don't want the context to reopen the same temp file.
 * Others?

 Ensure these considerations are reflected in your unit tests!

***Every file written in this class must be written atomically, via this
function or otherwise.***

### Parquet

Excel is a very poor file format compared to modern column stores.
In [__main__.py](pset_1/__main__.py) use
[Parquet via Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#parquet)
to transform the provided excel file into a better format.

The new file should keep the same name, but with the extension changed to
`.parquet`. The new file should keep the same path as the original file.

Ensure you use your atomic write (consider using `as_file=False` with
`.to_parquet`).

In [__main__.py](pset_1/__main__.py) read back ***just the hashed id column***
and print it. ***DO NOT*** read the entire data set!

### Your main script

Implement top level execution in [pset_1/\__main__.py](pset_1/__main__.py) to
show your work and answer the q's in the pset answers quiz.  It can be invoked
with `python -m pset_1`.
