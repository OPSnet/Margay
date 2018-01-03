import binascii


def hex_decode(inp: str) -> str:
    out = ''
    i = 0
    while i < len(inp):
        if inp[i] == '%' and (i + 2) < len(inp):
            # ord('0') == chr(48)
            x = 48

            i += 1
            char = ord(inp[i])
            if 'a' <= inp[i] <= 'f':
                x = (char - 87) << 4
            elif 'A' <= inp[i] <= 'F':
                x = (char - 55) << 4
            elif '0' <= inp[i] <= '9':
                x = (char - 48) << 4

            i += 1
            char = ord(inp[i])
            if 'a' <= inp[i] <= 'f':
                x += (char - 87)
            elif 'A' <= inp[i] <= 'F':
                x += (char - 55)
            elif '0' <= inp[i] <= '9':
                x += (char - 48)
            x = chr(x)
        else:
            x = inp[i]
        out += x
        i += 1
    return out


if __name__ == '__main__':
    #a = 'A959693FCAC904B7247537B3327740BFCEAF7851'
    #b = b'0xA959693FCAC904B7247537B3327740BFCEAF7851'
    #print(a)
    #print(b)
    test = hex_decode('%a9Yi%3f%ca%c9%04%b7%24u7%b32w%40%bf%ce%afxQ')
    print(''.join(hex(ord(c))[2:] for c in test))
    #print(binascii.hexlify(bytearray(, 'utf8')))
    #print(binascii.hexlify(hex_decode('%A9Yi?%CA%C9%04%B7$u7%B32w@%BF%CE%AFxQ')))
    #print(hashlib.sha1(hex_decode('%a9Yi%3f%ca%c9%04%b7%24u7%b32w%40%bf%ce%afxQ').encode('utf-8')).digest())

"""
>>> import urllib
>>> '%a9Yi%3f%ca%c9%04%b7%24u7%b32w%40%bf%ce%afxQ'
'%a9Yi%3f%ca%c9%04%b7%24u7%b32w%40%bf%ce%afxQ'
>>> urllib.unquote_plus(_)
'\xa9Yi?\xca\xc9\x04\xb7$u7\xb32w@\xbf\xce\xafxQ'
>>> _.encode('hex')
'a959693fcac904b7247537b3327740bfceaf7851'
>>> 'a959693fcac904b7247537b3327740bfceaf7851'
"""