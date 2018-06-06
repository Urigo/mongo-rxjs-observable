export default {
  output: {
    format: 'umd'
  },
  external: ['rxjs'],
  globals: {
    'meteor/meteor': 'Package.meteor',
    'meteor/mongo': 'Package.mongo',
    'meteor/tracker': 'Package.tracker'
  }
};
