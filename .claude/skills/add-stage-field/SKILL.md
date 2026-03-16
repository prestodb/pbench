---
name: add-stage-field
description: Use when adding a new parameter or field to the Stage struct in stage/stage.go, to ensure all required locations are updated
---

# Add Stage Field

When adding a new field to the `Stage` struct, you must update multiple locations. Missing any causes silent bugs (fields not inherited, not merged, missing defaults).

## Checklist

Update these locations in order:

1. **`stage/stage.go` — `Stage` struct**: Add the field with a `json:"field_name,omitempty"` tag. Use pointer types (`*bool`, `*int`, `*string`) for optional fields that participate in inheritance (nil = "not set, inherit from parent").

2. **`stage/stage_utils.go` — `MergeWith()`**: Add merge logic. For pointer fields: `if other.Field != nil { s.Field = other.Field }`. For slices: `s.Field = append(s.Field, other.Field...)`. For maps: iterate and merge key-by-key (nil value = delete key).

3. **`stage/stage_utils.go` — `setDefaults()`**: Set a default value if the field needs one (e.g., `false` for bool flags, `0` for counts). Only needed for fields that must have a non-nil value before execution.

4. **`stage/stage_utils.go` — `propagateStates()`**: Propagate to child stages if the field should be inherited. Pattern: `if nextStage.Field == nil { nextStage.Field = s.Field }`. Skip this if the field is stage-local only.

5. **`stage/stage.go` — `newStreamInstance()`**: Copy the field if it's relevant to stream (concurrent) execution. Most execution-related fields should be copied here.

6. **Wiki: [Parameters](https://github.com/prestodb/pbench/wiki/Parameters)**: Document the new parameter with its JSON key, type, default, and description.

7. **Wiki: [Configuring PBench - Inherited Parameters](https://github.com/prestodb/pbench/wiki/Configuring-PBench#inherited-parameters-in-stage-files)**: Add to the inherited list if propagated in step 4.

## Verification

After implementation, grep to confirm the field name appears in all required locations:

```bash
grep -n 'FieldName' stage/stage.go stage/stage_utils.go
```

Ensure the count matches expectations (struct definition + MergeWith + setDefaults if needed + propagateStates if inherited + newStreamInstance if stream-relevant).
