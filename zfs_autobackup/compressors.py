# Adopted from Syncoid :)

# this software is licensed for use under the Free Software Foundation's GPL v3.0 license, as retrieved
# from http://www.gnu.org/licenses/gpl-3.0.html on 2014-11-17.  A copy should also be available in this
# project's Git repository at https://github.com/jimsalterjrs/sanoid/blob/master/LICENSE.

COMPRESS_CMDS = {
    'gzip': {
        'cmd': 'gzip',
        'args': [ '-3' ],
        'dcmd': 'zcat',
        'dargs': [],
    },
    'pigz-fast': {
        'cmd': 'pigz',
        'args': [ '-3' ],
        'dcmd': 'pigz',
        'dargs': [ '-dc' ],
    },
    'pigz-slow': {
        'cmd': 'pigz',
        'args': [ '-9' ],
        'dcmd': 'pigz',
        'dargs': [ '-dc' ],
    },
    'zstd-fast': {
        'cmd': 'zstdmt',
        'args': [ '-3' ],
        'dcmd': 'zstdmt',
        'dargs': [ '-dc' ],
    },
    'zstd-slow': {
        'cmd': 'zstdmt',
        'args': [ '-19' ],
        'dcmd': 'zstdmt',
        'dargs': [ '-dc' ],
    },
    'xz': {
        'cmd': 'xz',
        'args': [],
        'dcmd': 'xz',
        'dargs': [ '-d' ],
    },
    'lzo': {
        'cmd': 'lzop',
        'args': [],
        'dcmd': 'lzop',
        'dargs': [ '-dfc' ],
    },
    'lz4': {
        'cmd': 'lz4',
        'args': [],
        'dcmd': 'lz4',
        'dargs': [ '-dc' ],
    },
}

def compress_cmd(compressor):
    ret=[ COMPRESS_CMDS[compressor]['cmd'] ]
    ret.extend( COMPRESS_CMDS[compressor]['args'])
    return ret

def decompress_cmd(compressor):
    ret= [ COMPRESS_CMDS[compressor]['dcmd'] ]
    ret.extend(COMPRESS_CMDS[compressor]['dargs'])
    return ret

def choices():
    return COMPRESS_CMDS.keys()
