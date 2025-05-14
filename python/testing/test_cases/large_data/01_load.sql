## metadata
### sync_timeout 3600
### query_timeout 300
### poll_interval 1

## test

-- load a large CSV file
CREATE TABLE mushroom_overload ("class" CHAR NOT NULL,
       	                        "cap-diameter" FLOAT NOT NULL,
				"cap-shape" CHAR NOT NULL,
				"cap-surface" CHAR,
				"cap-color" CHAR NOT NULL,
				"does-bruise-or-bleed" BOOLEAN NOT NULL,
				"gill-attachment" CHAR,
				"gill-spacing" CHAR,
				"gill-color" CHAR NOT NULL,
				"stem-height" FLOAT NOT NULL,
				"stem-width" FLOAT NOT NULL,
				"stem-root" CHAR,
				"stem-surface" CHAR,
				"stem-color" CHAR,
				"veil-type" CHAR,
				"veil-color" CHAR,
				"has-ring" BOOLEAN NOT NULL,
				"ring-type" CHAR,
				"spore-print-color" CHAR,
				"habitat" CHAR NOT NULL,
				"season" CHAR NOT NULL);
### load_csv mushroom_overload.csv mushroom_overload

## verify
-- query the number of rows in the table
SELECT count(*) FROM mushroom_overload;
