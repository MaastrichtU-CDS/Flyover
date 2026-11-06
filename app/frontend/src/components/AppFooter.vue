<template>
  <footer class="flyover-footer">
    <!-- The page content's container, as the status bar at the top uses, so
         the logo and the version line up with the store and the project. -->
    <div class="container container-flyover">
      <div class="row">
        <div class="col-md-4 text-md-left">
          <p class="footer-text-md">
            <a
              href="https://clinicaldatascience.nl/"
              target="_blank"
              rel="noopener"
            >
              <img
                src="/static/logo-cds.svg"
                alt="Clinical Data Science Maastricht"
                class="footer-logo"
              >
            </a>
          </p>
        </div>
        <div class="col-md-4 text-md-center">
          <p class="footer-text" />
        </div>
        <div class="col-md-4 text-md-right">
          <a
            href="https://github.com/MaastrichtU-CDS/Flyover"
            target="_blank"
            rel="noopener"
            class="footer-link"
          >
            <p class="footer-text">
              <i class="fas fa-code" />
              <strong>Flyover</strong> v3.5.0
            </p>
          </a>
        </div>
      </div>
    </div>
  </footer>
</template>
