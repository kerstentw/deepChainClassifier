import json
import db_info

SAMPLE = """
'0x0bb4251b8c5e8acb27d6809d77a10aa1cacc5144':
    {
      'id': 6604,
      'name': 'gdax.us',
      'url': 'http://gdax.us',
      'coin': 'ETH',
      'category': 'Scamming',
      'subcategory': 'Exchange',
      'description': 'Fake exchange (same as your-btc.co.uk)',
      'addresses': ['1gj9tymgjcfsnjprkdcaugemewix2ysbdl',
                    '0x0bb4251b8c5e8acb27d6809d77a10aa1cacc5144',
                    'qzh7l8neh3v868uzu74qpyat4gjz2rdv0yhjktqcp2'],
      'reporter': 'MyCrypto',
      'ip': '217.23.6.10',
      'nameservers': ['1a7ea920.bitcoin-dns.hosting',
                      'c358ea2d.bitcoin-dns.hosting',
                      'a8332f3a.bitcoin-dns.hosting',
                      'ad636824.bitcoin-dns.hosting'],
      'status': 'Offline'}

      """

SOURCE = "/Users/tk/Programming/Projects/DeepChain/remote_repo/deepChainClassifier/Recorder/raw_addrs/ethereum/scams/etherscamdb/scams.json"
