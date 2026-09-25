# From Using Spark to Understanding Spark

A 30-episode, experiment-led learning log by an MLE: from running PySpark
locally to understanding execution, small clusters, YARN, and production
troubleshooting.

## Principles

Each episode answers one concrete question, has one runnable experiment, and
records evidence (a Spark UI screenshot, an execution plan, a log excerpt, or
an output table).  The post should explain what changed in the experiment and
why it matters in an MLE workflow.

## Repository layout

```text
episodes/                 # One self-contained folder per published episode
  ep01-<slug>/
    README.md              # Reproduction instructions and short conclusion
    article.md             # Publishable Chinese article draft, written by you
    experiment/            # Python / SQL / config used by the episode
    screenshots/           # Committed evidence, never source data
docs/                     # Series plan and public project documentation
```

Raw or generated data lives in `data/` and is ignored by Git.  Each experiment
must generate its own small fixture or document where to obtain it.

## Daily workflow

```bash
# Ensure the environment is ready
uv sync

# Review and create a local commit
git status
git add episodes/ep01-local-pyspark-first-run
git commit -m "docs: publish ep01 local PySpark first run"

# After the GitHub remote is configured
git push
```

## First-time setup

```bash
uv sync
```

## Conventions

- Episode folders use `epNN-english-kebab-case`.
- `article.md` is the canonical article source.  Copy it to a blogging platform;
  do not make the platform the only copy.
- Never commit credentials, production logs, internal URLs, customer data, or
  unapproved screenshots.
- Commit source, configs, notes, and deliberately selected screenshots only.

## Environment phases

1. Ep01–17: local PySpark and Spark UI.
2. Ep18–21: Docker Spark standalone cluster.
3. Ep22–26: Docker YARN/HDFS learning cluster.
4. Ep27–30: integration; Kubernetes is optional and subject to company policy.

See [the series plan](docs/series-plan.md).
