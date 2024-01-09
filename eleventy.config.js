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
