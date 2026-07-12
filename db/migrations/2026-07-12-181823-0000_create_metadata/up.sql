CREATE TABLE connector_checkpoints (
    graph_id UUID NOT NULL,
    connector TEXT NOT NULL,
    accepted_sequence BIGINT NOT NULL CHECK (accepted_sequence >= 0),
    PRIMARY KEY (graph_id, connector)
);

CREATE TABLE graph_labels (
    graph_id UUID PRIMARY KEY,
    display_label TEXT NOT NULL UNIQUE
);

CREATE TABLE auth_material (
    key TEXT PRIMARY KEY,
    token_hash BYTEA NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);
