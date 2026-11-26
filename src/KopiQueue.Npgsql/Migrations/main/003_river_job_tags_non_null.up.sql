UPDATE {{schema}}{{prefix}}job SET tags = '{}' WHERE tags IS NULL;

ALTER TABLE {{schema}}{{prefix}}job
    ALTER COLUMN tags SET DEFAULT '{}',
    ALTER COLUMN tags SET NOT NULL;
