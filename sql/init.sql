CREATE SCHEMA IF NOT EXISTS source_schema;
CREATE SCHEMA IF NOT EXISTS target_schema;

CREATE TABLE IF NOT EXISTS public.users_stg (
    id          INTEGER      NOT NULL,
    email       VARCHAR(255) NOT NULL,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    avatar      TEXT,
    source      VARCHAR(20)  NOT NULL DEFAULT ''api'',
    loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS source_schema.users (
    id          SERIAL       PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    department  VARCHAR(100),
    hire_date   TIMESTAMPTZ,
    salary      NUMERIC(12,2),
    is_active   BOOLEAN      DEFAULT TRUE,
    loaded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS target_schema.users (
    id          INTEGER      NOT NULL,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    department  VARCHAR(100),
    hire_date   TIMESTAMPTZ,
    salary      NUMERIC(12,2),
    is_active   BOOLEAN      DEFAULT TRUE,
    copied_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.etl_watermarks (
    source_name  VARCHAR(50) PRIMARY KEY,
    last_run_at  TIMESTAMPTZ NOT NULL DEFAULT ''1970-01-01 00:00:00+00'',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.etl_watermarks (source_name) VALUES (''api''), (''file''), (''db'')
ON CONFLICT DO NOTHING;
