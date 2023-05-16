CREATE TABLE IF NOT EXISTS public.assets
(
    id integer NOT NULL DEFAULT nextval('assets_id_seq'::regclass),
    ticker text COLLATE pg_catalog."default" NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    ppi_id text COLLATE pg_catalog."default",
    CONSTRAINT assets_pkey PRIMARY KEY (id),
    CONSTRAINT ticker UNIQUE (ticker)
)

TABLESPACE pg_default;

ALTER TABLE public.assets
    OWNER to postgres;
	
CREATE TABLE IF NOT EXISTS public.history
(
    id integer NOT NULL DEFAULT nextval('history_id_seq'::regclass),
    asset_id integer NOT NULL,
    date date NOT NULL,
    opening_value numeric(12,4),
    closing_value numeric(12,4),
    min_value numeric(12,4),
    max_value numeric(12,4),
    volume bigint,
    prev_close numeric(12,4),
    CONSTRAINT history_pkey PRIMARY KEY (id),
    CONSTRAINT asset_fk FOREIGN KEY (asset_id)
        REFERENCES public.assets (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE public.history
    OWNER to postgres;
	

CREATE TABLE IF NOT EXISTS public.intra
(
    id integer NOT NULL DEFAULT nextval('intra_id_seq'::regclass),
    asset_id integer NOT NULL,
    datetime timestamp without time zone,
    value numeric(12,4),
    volume numeric(12,2),
    CONSTRAINT intra_pkey PRIMARY KEY (id),
    CONSTRAINT asset_fk FOREIGN KEY (asset_id)
        REFERENCES public.assets (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE public.intra
    OWNER to postgres;


CREATE TABLE IF NOT EXISTS orders
(
    id SERIAL NOT NULL,
	datetime TIMESTAMP NOT NULL,
    asset_id Integer NOT NULL,
    order_type Text COLLATE pg_catalog."default" NOT NULL,
    order_status Text COLLATE pg_catalog."default" NOT NULL,
	nominals Integer NOT NULL,
	done_nominals Integer NOT NULL,
	price Numeric(12,4),
	order_id Integer,
    CONSTRAINT order_pkey PRIMARY KEY (Id),
	CONSTRAINT Asset_FK FOREIGN KEY (asset_id) REFERENCES assets (id) ON DELETE RESTRICT
)

TABLESPACE pg_default;

ALTER TABLE public.orders
    OWNER to postgres;
