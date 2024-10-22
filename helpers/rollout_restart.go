// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package helpers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	argorolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/hashicorp/vault-secrets-operator/api/v1beta1"
	"github.com/hashicorp/vault-secrets-operator/consts"
)

// AnnotationRestartedAt is updated to trigger a rollout-restart
const AnnotationRestartedAt = "vso.secrets.hashicorp.com/restartedAt"

// HandleRolloutRestarts for all v1beta1.RolloutRestartTarget(s) configured for obj.
// Supported objs are: v1beta1.VaultDynamicSecret, v1beta1.VaultStaticSecret, v1beta1.VaultPKISecret
// Please note the following:
// - a rollout-restart will be triggered for each configured v1beta1.RolloutRestartTarget
// - the rollout-restart action has no support for roll-back
// - does not wait for the action to complete
//
// Returns all errors encountered.
func HandleRolloutRestarts(ctx context.Context, client ctrlclient.Client, obj ctrlclient.Object, recorder record.EventRecorder) error {
	logger := log.FromContext(ctx)

	var targets []v1beta1.RolloutRestartTarget
	switch t := obj.(type) {
	case *v1beta1.VaultDynamicSecret:
		targets = t.Spec.RolloutRestartTargets
	case *v1beta1.VaultStaticSecret:
		targets = t.Spec.RolloutRestartTargets
	case *v1beta1.VaultPKISecret:
		targets = t.Spec.RolloutRestartTargets
	case *v1beta1.HCPVaultSecretsApp:
		targets = t.Spec.RolloutRestartTargets
	default:
		err := fmt.Errorf("unsupported Object type %T", t)
		recorder.Eventf(obj, corev1.EventTypeWarning, consts.ReasonRolloutRestartUnsupported,
			"Rollout restart impossible (please report this bug): err=%s", err)
		return err
	}

	if len(targets) == 0 {
		return nil
	}

	var errs error
	for _, target := range targets {
		if err := RolloutRestart(ctx, obj.GetNamespace(), target, client); err != nil {
			errs = errors.Join(err)
			recorder.Eventf(obj, corev1.EventTypeWarning, consts.ReasonRolloutRestartFailed,
				"Rollout restart failed for target %#v: err=%s", target, err)
		} else {
			recorder.Eventf(obj, corev1.EventTypeNormal, consts.ReasonRolloutRestartTriggered,
				"Rollout restart triggered for %v", target)
		}
	}

	if errs != nil {
		logger.Error(errs, "Rollout restart failed", "targets", targets)
	} else {
		logger.V(consts.LogLevelDebug).Info("Rollout restart succeeded", "total", len(targets))
	}

	return errs
}

// RolloutRestart patches the target in namespace for rollout-restart.
// Supported target Kinds are: DaemonSet, Deployment, StatefulSet
func RolloutRestart(ctx context.Context, namespace string, target v1beta1.RolloutRestartTarget, client ctrlclient.Client) error {
	if namespace == "" {
		return fmt.Errorf("namespace cannot be empty")
	}

	objectMeta := metav1.ObjectMeta{
		Namespace: namespace,
		Name:      target.Name,
	}

	var obj ctrlclient.Object
	switch target.Kind {
	case "DaemonSet":
		obj = &appsv1.DaemonSet{
			ObjectMeta: objectMeta,
		}
	case "Deployment":
		obj = &appsv1.Deployment{
			ObjectMeta: objectMeta,
		}
	case "StatefulSet":
		obj = &appsv1.StatefulSet{
			ObjectMeta: objectMeta,
		}
	case "argo.Rollout":
		obj = &argorolloutsv1alpha1.Rollout{
			ObjectMeta: objectMeta,
		}
	default:
		// Support any fully qualified Kubernetes Group, Kind, Version
		// Split target.Kind to separate the kind from the group (e.g., "kafka.strimzi.io/Kafkaconnect")
		parts := strings.Split(target.Kind, "/")
		if len(parts) != 2 {
			return fmt.Errorf("invalid Kind format: %s, expected <group>/<kind>", target.Kind)
		}
		kind, group := parts[1], parts[0]

		// Construct the GVK (GroupVersionKind) for the target object
		gvk := schema.GroupVersionKind{
			Group:   group,
			Kind:    kind,
			Version: "v1beta2",
		}

		// Create a generic unstructured object with the appropriate GVK
		uObj := &unstructured.Unstructured{}
		uObj.SetGroupVersionKind(gvk)
		uObj.SetNamespace(namespace)
		uObj.SetName(target.Name)
		obj = uObj
	}

	return patchForRolloutRestart(ctx, obj, client)
}

func isDeploymentPaused(obj interface{}) bool {
	// Get the reflect.Value of the object
	val := reflect.ValueOf(obj).Elem()

	// Access the Spec field
	specField := val.FieldByName("Spec")
	if !specField.IsValid() {
		// Spec field does not exist
		return false
	}

	// Access the Paused field in Spec
	pausedField := specField.FieldByName("Paused")
	if !pausedField.IsValid() || pausedField.Kind() != reflect.Bool {
		// Paused field does not exist or is not a boolean
		return false
	}

	// Return the value of the Paused field
	return pausedField.Bool()
}

func patchForRolloutRestart(ctx context.Context, obj ctrlclient.Object, client ctrlclient.Client) error {
	objKey := ctrlclient.ObjectKeyFromObject(obj)
	if err := client.Get(ctx, objKey, obj); err != nil {
		return fmt.Errorf("failed to Get object for objKey %s, err=%w", objKey, err)
	}

	// Check for the Kind of the object
	gvk := obj.GetObjectKind().GroupVersionKind()

	if gvk.Kind == "Deployment" && isDeploymentPaused(obj) {
		return fmt.Errorf("deployment %s is paused, cannot restart it", objKey)
	}

	// Handle Argo Rollout with a spec.RestartAt field
	if gvk.GroupKind().String() == "Rollout.argoproj.io" {
		if uObj, ok := obj.(*unstructured.Unstructured); ok {
			patch := ctrlclient.MergeFrom(uObj.DeepCopy())
			restartAt := metav1.Now().Format(time.RFC3339)
			if err := unstructured.SetNestedField(uObj.Object, restartAt, "spec", "restartAt"); err != nil {
				return fmt.Errorf("failed to set spec.RestartAt: %w", err)
			}

			return client.Patch(ctx, uObj, patch)
		}
	}

	// Handle KafkaConnect with a spec.template.pod field
	if gvk.GroupKind().String() == "KafkaConnect.kafka.strimzi.io" {
		if uObj, ok := obj.(*unstructured.Unstructured); ok {
			// Check for the "spec" field in the unstructured object
			spec, found, err := unstructured.NestedMap(uObj.Object, "spec")
			if err != nil || !found {
				return fmt.Errorf("spec field not found or error: %v", err)
			}

			// Check or update annotations
			annotations, found, err := unstructured.NestedStringMap(spec, "template", "pod", "metadata", "annotations")
			if err != nil || !found {
				annotations = make(map[string]string)
			}

			// Add or update the restartedAt annotation
			annotations[AnnotationRestartedAt] = time.Now().Format(time.RFC3339)

			patch := ctrlclient.MergeFrom(uObj.DeepCopy())

			// Set the annotations back in the unstructured object
			err = unstructured.SetNestedStringMap(uObj.Object, annotations, "spec", "template", "pod", "metadata", "annotations")
			if err != nil {
				return fmt.Errorf("failed to set annotations: %v", err)
			}

			return client.Patch(ctx, uObj, patch)
		}
	}

	// Use reflection for typed objects (e.g., Deployment, StatefulSet, DaemonSet)
	v := reflect.ValueOf(obj).Elem()

	// Check if the object has the Spec.Template.ObjectMeta.Annotations field
	specField := v.FieldByName("Spec")
	if !specField.IsValid() {
		return fmt.Errorf("object %T does not have a Spec field", obj)
	}

	// Access template and annotations fields using reflection
	templateField := specField.FieldByName("Template")
	if !templateField.IsValid() {
		return fmt.Errorf("object %T does not have a Spec.Template field", obj)
	}

	metaField := templateField.FieldByName("ObjectMeta")
	if !metaField.IsValid() {
		return fmt.Errorf("object %T does not have a Spec.Template.ObjectMeta field", obj)
	}

	annotationsField := metaField.FieldByName("Annotations")
	if !annotationsField.IsValid() || annotationsField.Kind() != reflect.Map {
		return fmt.Errorf("object %T does not have Annotations in Spec.Template.ObjectMeta", obj)
	}

	// Ensure the annotations map is initialized
	if annotationsField.IsNil() {
		annotationsField.Set(reflect.MakeMap(annotationsField.Type()))
	}

	// Set the restartedAt annotation
	annotationsField.SetMapIndex(reflect.ValueOf(AnnotationRestartedAt), reflect.ValueOf(time.Now().Format(time.RFC3339)))

	// Create a deep copy and cast it back to ctrlclient.Object
	patch := ctrlclient.StrategicMergeFrom(obj.DeepCopyObject().(ctrlclient.Object))

	// Apply the patch
	return client.Patch(ctx, obj, patch)
}
