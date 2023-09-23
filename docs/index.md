---
homepage: true
layout: product
title: Ingest large amounts of data into PostgreSQL
description: Use this Python package to ingest data into a SQLAlchemy-defined PostgreSQL table, leveraging high-watermarking to keep it up to date without re-ingesting the same data.
startButton:
  href: "get-started"
  text: Get started
---


<div class="govuk-grid-row">
  <section class="govuk-grid-column-one-third-from-desktop govuk-!-margin-bottom-7">
    <h2 class="govuk-heading-m govuk-!-font-size-27">Auto migrations</h2>
    <p class="govuk-body">Existing tables are automatically migrated as needed - no need to write and run migrations separately to the ingest themselves.</p>
  </section>
  <section class="govuk-grid-column-one-third-from-desktop govuk-!-margin-bottom-7">
    <h2 class="govuk-heading-m govuk-!-font-size-27">Memory efficient</h2>
    <p class="govuk-body">The API is iterable-based to support streaming large amounts of data into PostgreSQL without loading it all into memory.</p>
  </section>
  <section class="govuk-grid-column-one-third-from-desktop govuk-!-margin-bottom-7">
    <h2 class="govuk-heading-m govuk-!-font-size-27">Performance</h2>
    <p class="govuk-body">Under the hood the PostgreSQL COPY statement is used to make ingests as performant as possible.</p>
  </section>
</div>

<hr class="govuk-section-break govuk-section-break--visible govuk-section-break--xl govuk-!-margin-top-0">

<div class="govuk-grid-row">
  <section class="govuk-grid-column-two-thirds">
    <h2 class="govuk-heading-m govuk-!-font-size-27">Contributions</h2>
    <p class="govuk-body">The code for pg-bulk-ingest is public and contributions are welcome though the <a class="govuk-link govuk-!-font-weight-bold" href="https://github.com/uktrade/pg-bulk-ingest">pg-bulk-ingest repository on GitHub</a>.</p>
  </section>
</div>
