use buffa::Message;
use buffa::MessageField;
use buffa::MessageView;
use types::domain;

use crate::protobuf;

#[test]
fn nested_conversion_prepends_its_field() {
    let request = protobuf::v1::CreateComponentRequest {
        component_id: Some([1; 16].to_vec()),
        spec: MessageField::some(protobuf::v1::NewComponentSpec {
            connector: Some("k8s".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let bytes = request.encode_to_vec();
    let request = protobuf::v1::CreateComponentRequestView::decode_view(&bytes).unwrap();

    let error = domain::NewComponent::try_from(&request).unwrap_err();

    assert_eq!(error.to_string(), "missing required field spec.name");
}

#[test]
fn repeated_conversion_includes_the_item_index() {
    let request = protobuf::v1::UpdateComponentsRequest {
        graph_id: Some([1; 16].to_vec()),
        expected_generation: Some(1),
        replacements: vec![protobuf::v1::ComponentReplacement {
            current_component_id: Some([2; 16].to_vec()),
            replacement_component_id: Some(vec![3]),
            ..Default::default()
        }],
        request_id: Some([4; 16].to_vec()),
        ..Default::default()
    };
    let bytes = request.encode_to_vec();
    let request = protobuf::v1::UpdateComponentsRequestView::decode_view(&bytes).unwrap();

    let error = domain::UpdateComponents::try_from(&request).unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid field replacements[0].replacement_component_id: must contain exactly 16 bytes, \
         received 1"
    );
}

#[test]
fn domain_validation_identifies_its_source_field() {
    let graph = protobuf::v1::Graph {
        id: Some([1; 16].to_vec()),
        generation: Some(0),
        ..Default::default()
    };
    let bytes = graph.encode_to_vec();
    let graph = protobuf::v1::GraphView::decode_view(&bytes).unwrap();

    let error = domain::Graph::try_from(&graph).unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid field generation: graph generation must be greater than zero"
    );
}
