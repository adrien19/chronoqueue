window.addEventListener('load', function () {
  SwaggerUIBundle({
    url: '/docs/swagger.json',
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
    plugins: [SwaggerUIBundle.plugins.DownloadUrl],
    layout: 'StandaloneLayout',
    docExpansion: 'list',
    defaultModelExpandDepth: 3,
    defaultModelsExpandDepth: 1,
    tryItOutEnabled: true,
    validatorUrl: null
  });
});
