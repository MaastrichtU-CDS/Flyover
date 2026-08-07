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
  });
});
