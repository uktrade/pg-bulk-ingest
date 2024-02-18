const govukEleventyPlugin = require('@x-govuk/govuk-eleventy-plugin')
const fs = require('fs')

module.exports = function(eleventyConfig) {
  // Register the plugin
  eleventyConfig.addPlugin(govukEleventyPlugin, {
    icons: {
      shortcut: '/assets/dit-favicon.png'
    },
    header: {
      logotype: {
        html: fs.readFileSync('./docs/assets/dit-logo.svg', {encoding: 'utf8'})
      },
      productName: 'pg-bulk-ingest',
    },
    footer: {
      meta: {
        items: [
          {
            href: 'https://github.com/uktrade/pg-bulk-ingest',
            text: 'GitHub repository for pg-bulk-ingest'
          },
          {
            href: 'https://www.gov.uk/government/organisations/department-for-business-and-trade',
            text: 'Created by the Department for Business and Trade (DBT)'
          }
        ]
      }
    }
  })

  eleventyConfig.addPassthroughCopy('./docs/assets')
  eleventyConfig.addPassthroughCopy('./docs/CNAME')

  return {
    dataTemplateEngine: 'njk',
    htmlTemplateEngine: 'njk',
    markdownTemplateEngine: 'njk',
    dir: {
      // Use layouts from the plugin
      input: 'docs',
      layouts: '../node_modules/@x-govuk/govuk-eleventy-plugin/layouts'
    }
  }
};
