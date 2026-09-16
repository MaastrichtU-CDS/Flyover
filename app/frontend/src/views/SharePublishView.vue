<script setup>
import { ref } from 'vue'

const jsonldFileName = ref('')
const jsonldUploaded = ref(false)
</script>

<template>
  <div>
    <br>
    <h1><i class="fas fa-cloud-upload-alt" /> Publish Your Data</h1>
    <hr>
    <p style="line-height: 1.5; margin: 0">
      Share your semantically interoperable data and metadata with the broader research
      community.
    </p>
    <br>

    <div id="codeBookSection">
      <div class="card mb-4">
        <div class="card-header">
          <h5 class="mb-0">
            <i class="fas fa-book" /> Create an Interactive Codebook
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted">
            Create a comprehensive codebook documenting your project's variables, values,
            and relationships using your semantic mappings. This structured overview helps
            others understand and work with your data.
          </p>
          <p class="text-muted">
            See an example codebook
            <a
              href="https://strongaya.github.io/AYA-cancer-semantic-map/"
              target="_blank"
              rel="noopener"
            >here</a>
            (Apache-2 licenced). Repository available
            <a
              href="https://github.com/STRONGAYA/AYA-cancer-semantic-map"
              target="_blank"
              rel="noopener"
            >here</a>.
          </p>
          <div class="alert alert-info info-purple">
            <i class="fas fa-info-circle" />
            <strong>Benefits of hosting mappings in Git</strong><br>
            Hosting your mappings on a Git-tracked system is a great way to keep track of
            changes in your mapping over time, and to easily share updates with your
            collaborators.
          </div>
        </div>
      </div>
      <br>
    </div>

    <div id="healthDcatApTriplesSection">
      <div class="card mb-4">
        <div class="card-header">
          <h5 class="mb-0">
            <i class="fas fa-file-medical" /> Health DCAT-AP Metadata Export — under
            construction
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted">
            Generate Health DCAT-AP metadata describing your project's variables, values,
            and relationships. This format ensures EHDS compliance and enables
            standardised health data sharing.
          </p>

          <div class="alert alert-info info-purple">
            <i class="fas fa-info-circle" />
            <strong>What is a Health DCAT-AP file?</strong><br>
            A standardised metadata format based on the DCAT Application Profile for
            health data, providing a structured way to describe datasets, their variables,
            and the relationships between them.
          </div>

          <hr>
          <h6><i class="fas fa-upload" /> JSON-LD Semantic Map</h6>
          <p class="text-muted mb-3">
            To generate Health DCAT-AP triples, you need a Flyover semantic mapping.
          </p>

          <button
            type="button"
            class="btn btn-outline-primary disabled"
            disabled
          >
            <i class="fas fa-upload" /> Upload JSON-LD
          </button>
          <button
            type="button"
            class="btn btn-outline-secondary disabled"
            disabled
          >
            <i class="fas fa-database" /> Use Stored JSON-LD
          </button>

          <div
            v-if="jsonldUploaded"
            class="alert alert-success mt-3"
          >
            <i class="fas fa-check-circle" />
            <strong>JSON-LD semantic map ready!</strong>
            {{ jsonldFileName }}
          </div>
        </div>
      </div>
      <br>
    </div>

    <RouterLink
      to="/share"
      class="btn btn-light"
    >
      <i class="fas fa-arrow-left" /> Return to Share Options
    </RouterLink>
    <br><br>
  </div>
</template>

<style scoped>
.info-purple {
  font-size: 0.85em;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: linear-gradient(
    135deg,
    rgba(102, 126, 234, 0.75) 0%,
    rgba(118, 75, 162, 0.75) 100%
  );
  color: white;
}
</style>
