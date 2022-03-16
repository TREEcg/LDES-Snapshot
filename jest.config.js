module.exports = {
    clearMocks: true,
    moduleFileExtensions: ['ts', 'js'],
    roots: ['<rootDir>/test/'],
    testEnvironment: 'node',
    transform: {
        '^.+\\.ts?$': 'ts-jest',
    },
    globals: {
        'ts-jest': {
            diagnostics: false,
        },
    },
    collectCoverage: true,
    coverageReporters: ['text', 'lcov'],
    coveragePathIgnorePatterns: [
        '/dist/',
        '/node_modules/',
        '/test/',
        '/src/util/Vocabularies.ts',
        '/src/util/Conversion.ts',
        '/src/util/TimestampUtil.ts',
        'src/logging'
    ],
    moduleNameMapper: {
        '^jose/(.*)$': '<rootDir>/node_modules/jose/dist/node/cjs/$1',
    },
    testTimeout: 60000,
}
