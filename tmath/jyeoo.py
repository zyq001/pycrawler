#!/usr/bin/python
# -*- coding: UTF-8 -*-
import json

from networkHelper import getContentWithUA


def fromPapper():

    papperListUrl = 'http://api.jyeoo.com/v1/math2/report?b=0&s=0&g=%s&t=0&e=0&r=&y=0&x=&pi=%s&ps=20&po=2'
    papperDetailBaseUrl = 'http://api.jyeoo.com/v1/math2/report/%s?ia=false'
    questDetailUrl = 'http://api.jyeoo.com/math2/AppTag/GetQues/%s'

    AuthCode = 'Token 9F5BBF8F752F060B00D38F7C81686852695A463CD5661FE0848CBEADB3ACFD5EE96B0D3FB81C8FEB1' \
               'EEC4F7CDA82D9DEE1603C3FED9A10DCD04FF9A5D4A677589F8891C0CA24ECB55A50EA11FFE8AA1B6F389D23A42B' \
               '46E9529444F65FC72870C19AA1299F39C3809B3FB1C8D12B4C5E179FF3DA7ADB9AF5F8D40C95FC5418FE30CE3D884A' \
               '52DA1CCC9AAB43AC1DCC501FBE1936820E5D73'

    Cookie = 'jyean=5x1Tibi9YuqrgS_gvFCeIr04zbNLyVJh9OK3GtKXzwblw1fjXNmqyzh5Facz4VuQKP20e1BJjK' \
             'PgCqVHWiQ7nlBmyQoYE3JEwEFmlp60djjncxNj2m4iSwj8YnOHXa0p0;jy=6B882BD2C4626BDCBD' \
             '1A156DAEF3B6A149459A79829709858932A0831D669F2E504EB7C714F40360013A05356D0F284759128FC' \
             '09556AA4C66DB25AF5F6C5E8CA16BB1D5261C4B4C74C002D90BE6C0103D6B80DC270249B19D933EFED5E85651E2817' \
             '5AD8FDE7C21D6373B64C8276A4E25D88987C37AC54A91A8A44888540B25163F05F330F8E7A88991394DAFE124159DE8407' \
             'C8256AE9AE7CE4C8937AF95418BA780A2AEB99EF452B60E765A607BDCF94CF605D5D3BD058E9BE846875E6C2E2A587BE80' \
             '55436E5FD290661F6F3FEB41EF00CA118E16E13B42F509ED690F7038DA498DA9EA0A39E5F6A377E409A5230CA67C9B7C' \
             'A9A00B3356D77346878D2B78188D1F3D17F48619D51D6C9158C6491C96423357206B7BDF1FFD7A2C4A34C334F8EE97ED' \
             '32FE7E075315375AAACDEC9B8AA17AF3F367827930B803BD060A685F8693E318F7782663D9C18F84753229011B6D' \
             '356BD26835F31CAD0F65B1DE78D915FE08D2FBEA480574BF9431C2DF9AD;'
    header = dict()
    header['Authorization'] = AuthCode
    header['Cookie'] = Cookie

    for g in range(1,13):
        for pageSize in range(0, 150):
            pListContent = getContentWithUA(papperListUrl % (g,pageSize), headers=header)
            if pListContent:
                pListJson = json.loads(pListContent)
                for papper in pListJson['Data']:
                    ppId = papper['ID']

                    papperDetailContent = getContentWithUA(papperDetailBaseUrl % ppId, headers=header)
                    if not papperDetailContent:
                        print 'papper detail failed continue'
                        continue
                    papperDetailJson = json.loads(papperDetailContent)

                    ppTitle = papperDetailJson['Title']
                    Score = papperDetailJson['Score']
                    SchoolName = papperDetailJson['SchoolName']
                    Degree = papperDetailJson['Degree']

                    for partJson in papperDetailJson['Groups']:
                        partName = partJson['Key']

                        # for articleJosn in partJson['Value']:

fromPapper()