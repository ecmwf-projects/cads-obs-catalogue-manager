FROM condaforge/mambaforge

WORKDIR /src/

COPY .. /src/
RUN mamba install -y gcc python=3.10 \
    && mamba env update -n base -f environment.yml \
    && mamba clean -afy


RUN pip install --no-deps -e .
