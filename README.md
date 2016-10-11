![Collaboratory logo](https://avatars1.githubusercontent.com/u/10966125?v=3&s=200)
![GA4GH logo](http://ga4gh.org/assets/svg/GA_logo.svg)

# Collaboratory - GA4GH

A GA4GH compliant API implementation to serve data from the [Collaboratory](http://www.cancercollaboratory.org/).

## Status

Currently only the [References API](http://ga4gh-schemas.readthedocs.io/en/latest/api/references.html) is partially supported. Future will will focus on the [Variants API](http://ga4gh-schemas.readthedocs.io/en/latest/api/variants.html), followed by the [Reads API](http://ga4gh-schemas.readthedocs.io/en/latest/api/reads.html). 

## Modules

The system is comprised of the following modules:

- [ga4gh-schema](ga4gh-schema/README.md)
- [ga4gh-server](ga4gh-server/README.md)
- [ga4gh-loader](ga4gh-loader/README.md)

## Build

From the command line:

```shell
mvn clean package
```

## Resources

- https://github.com/ga4gh/schemas
- http://ga4gh-schemas.readthedocs.io/en/latest/