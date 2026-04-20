const globals = require('globals');
const js = require('@eslint/js');

module.exports = [
  js.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: 'script',
      globals: {
        ...globals.browser,
        firebase: 'readonly',
        firestore: 'readonly',
        _GAMES_DATA: 'readonly',
        _XLSX_REVIEWS_DATA: 'readonly',
        _MEMBERS_LIST_DATA: 'readonly',
        _MEMBER_STATS_DATA: 'readonly',
      },
    },
    rules: {
      'no-unused-vars': ['warn', { args: 'none', varsIgnorePattern: '^_' }],
      'no-empty': ['warn', { allowEmptyCatch: true }],
      'no-prototype-builtins': 'off',
      'no-inner-declarations': 'off',
    },
  },
  {
    files: ['data/**/*.js'],
    rules: {
      'no-redeclare': 'off',
      'no-unused-vars': 'off',
    },
  },
  {
    ignores: ['node_modules/', 'data/*.json'],
  },
];
