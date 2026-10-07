export interface ProvinceProperties {
  ADM1_PCODE: string;
  ADM1_EN: string;
}

export interface DistrictProperties {
  DISTRICT: string;
}

export interface MunicipalityProperties {
  id: number | null;
  F_ID: number;
  N_ID: string;
  NAME: string;
  LEVEL: "Mahanagarpalika" | "Upa-Mahanagarpalika" | "Nagarpalika" | "Gaunpalika" | string;
  DISTRICT: string;
}

export interface GeoTag {
  id: string;
  label: string;
  note: string;
  lat: number;
  lng: number;
  district?: string;
  createdAt: string;
}
