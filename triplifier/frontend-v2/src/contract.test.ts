import { describe, expect, it } from "vitest";
import fixtureText from "../../../docs/v2/fixtures/omop-wide-requirement.jsonld?raw";

describe("v2 JSON-LD contract fixture", () => {
  it("round-trips without changing public extension fields", () => {
    const fixture = JSON.parse(fixtureText);
    const roundTrip = JSON.parse(JSON.stringify(fixture));
    expect(roundTrip.formatVersion).toBe("2.0");
    expect(roundTrip.targets.omop.cdmVersion).toBe("5.4");
    expect(roundTrip.targets.omop.variables.weight.domain).toBe("Measurement");
    expect(roundTrip.targets.omop.person.sexAtBirth).toBe("biological_sex");
    expect(roundTrip.schema.variables.biological_sex.valueMapping.terms.female.targetClass).toBe("gender:F");
    expect(roundTrip.targets.omop.variables.clinical_t.domain).toBe("Observation");
    expect(roundTrip.targets.omop.variables.clinical_t.valueMode).toBe("concept");
    expect(roundTrip.schema.variables.clinical_t.class).toBe("loinc:21905-5");
    expect(roundTrip.schema.variables.clinical_t.valueMapping.terms.cT1.targetClass).toBe("snomed:1228889001");
  });
});
