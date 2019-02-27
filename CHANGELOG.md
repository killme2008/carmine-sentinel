# Change Log

## [0.3.0] - 2019-02-27
### Compatibility Breaking Change
- Change defined sentinel commands format to be compatible with `carmine` version post `2.14`: will only work for `2.15`+ !

### Fixed
- carmine server connection settings are no longer lost: password connection works!

### Changed
- Expanded hard to read use of macros
- Change variable names from acronyms to full words from acronyms
- `try-resolve-master-spec` now accepts `server-conn` argument
- `get-sentinel-redis-spec` now checks arguments using `:pre` not `if ... throw`
