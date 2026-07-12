// @generated automatically by Diesel CLI.

diesel::table! {
    auth_material (key) {
        key -> Text,
        token_hash -> Bytea,
        enabled -> Bool,
    }
}

diesel::table! {
    connector_checkpoints (graph_id, connector) {
        graph_id -> Uuid,
        connector -> Text,
        accepted_sequence -> Int8,
    }
}

diesel::table! {
    graph_labels (graph_id) {
        graph_id -> Uuid,
        display_label -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(auth_material, connector_checkpoints, graph_labels,);
