import lxml.etree as ET
from dateutil.parser import parse
from pathlib import Path
from datetime import datetime
from unicodedata import (
    normalize,
    combining
)
from typing import Generator, Any
import os
import pyarrow as pa
import pandas as pd
from io import BytesIO


class FileXml:
    def __init__(
        self,
        string_xml: str,
        chave: str,
        instatus: int,
        dtgravacao: datetime,
        controle: str
    ) -> None:
        
        self.string_xml = string_xml
        self.chave = chave
        self.instatus = instatus
        self.dtgravacao = dtgravacao
        self.controle = controle
    
    def export_file_xml(
        self, 
        sub_path: str = None,
        memory: bool = True
    ) -> tuple[str, BytesIO] | tuple[str, None]:
        
        path_raiz = f'{self.controle}/{self.dtgravacao:%Y/%m/%d}'
        if sub_path:
           path_raiz = f'{sub_path}/{path_raiz}'

        file_to = (
            f'{path_raiz}/'
            f'{self.chave}_{self.instatus:02d}'
            f'_{self.dtgravacao:%Y%m%d}.xml'
        )
        
        root = ET.fromstring(self.string_xml)
        etree = ET.ElementTree(root)

        if memory:
            file_xml = BytesIO()
            etree.write(file_xml, pretty_print=True)
            file_xml.seek(0)
            return file_to, file_xml
        
        os.makedirs(path_raiz, exist_ok=True)
        with open(file_to, mode='wb') as f:
            etree.write(f, pretty_print=True)
        
        return file_to, None


class ParseXml:
    def __init__(
        self, 
        xml: str | Path | BytesIO, 
        namespace: str = '{http://www.portalfiscal.inf.br/nfe}'
    ) -> None:
        
        self.xml = xml
        self.namespace = namespace
        self.root = self.__get_root()

    def __get_root(self):
        root = ET.parse(self.xml)
        for elm in root.iter():
            elm.tag = elm.tag[len(self.namespace):]

        return root
    
    def ___header_note(self) -> dict:
        map_cab = {
            'chave': ('infNFe/@Id', lambda x: str.strip(x[3:])),
            'dh_emi': ('infNFe/ide/dhEmi', lambda x: parse(x[:10]).date()),
            'cnpj_origem': ('infNFe/emit/CNPJ', str),
            'cnpj_destino': ('infNFe/dest/CNPJ', str),
            'natureza_operacao': ('infNFe/ide/natOp', str),
            'numero_nota': ('infNFe/ide/nNF', int),
            'valor_nota': ('infNFe/total//vNF', float),
            'valor_base_icms': ('infNFe/total//vBC', float),
            'valor_icms': ('infNFe/total//vICMS', float),
            'serie': ('infNFe/ide/serie', int),
            'chave_estorno': ('infNFe/ide//refNFe', str)
        }

        output = dict()

        for key, value in map_cab.items():
            expr, func = value

            if (search := self.root.xpath(expr)):
                [tag] = search
                output[key] = func(getattr(tag, 'text', tag))
        
        return output
    
    def __detail_note(self) -> Generator[dict, Any, None]:
        
        cols_det = {
            'cProd':('cod', lambda x: int(x.replace('-', ''))),
            'cEAN': ('cod_barra', str),
            'xProd': ('nome_prod', str),
            'NCM': ('ncm', str),
            'CFOP': ('cfop', int),
            'uCom': ('und_comercial', str),
            'qCom': ('qtd', lambda x: int(float(x))),
            'vUnCom': ('vl_unit', float),
            'vDesc': ('vl_desc', float),
            'vProd': ('vl_total', float),
            'vBC': ('vl_base_icms', float),
            'pICMS': ('perc_icms', float),
            'vICMS': ('vl_icms', float),
            'orig': ('orig', int)
        }
   
        for child in self.root.findall('infNFe/det'):
            data = self.___header_note()
    
            for elm in child.iter():
                if elm.tag == 'det':
                    data['item'] = int(elm.get('nItem'))
    
                if elm.tag in cols_det.keys():
                    key, func = cols_det[elm.tag]
    
                    if elm.tag == 'vBC':
                        if elm.getparent().getparent().tag == 'ICMS':
                            data[key] = func(elm.text)
                    else:
                        data[key] = func(elm.text)
    
            yield data

    def records(self) -> list[dict[str, Any]]:
        return self.__detail_note()
    
    def arrow(self) -> pa.Table:
        return pa.Table.from_pylist([*self.__detail_note()])
    
    def df(self) -> pd.DataFrame:
        return pd.DataFrame.from_records(self.__detail_note(), coerce_float=True)