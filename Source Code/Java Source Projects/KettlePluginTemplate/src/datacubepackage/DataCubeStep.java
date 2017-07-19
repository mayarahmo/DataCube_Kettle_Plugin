package datacubepackage;

import java.util.List;

/*
 * Feito por: GABRIEL GONÇALVES DE CASTRO MARQUES
 * ALUNO DO CURSO DE CIÊNCIA DA COMPUTAÇÃO DA UFRJ
 * 05/05/15
 * DATACUBE TRANSFORMATOR PARA KETTLE(SPOON)
 */


import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import br.ufrj.ppgi.greco.kettle.plugin.tools.datatable.DataTable;

public class DataCubeStep extends BaseStep implements
        StepInterface
{

	int j = 0;
    public DataCubeStep(StepMeta stepMeta,
            StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans)
    {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi)
    {
        if (super.init(smi, sdi))
        {
            // TODO init something here if needed
            // ...
            return true;
        }
        else
            return false;
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        super.dispose(smi, sdi);

        // TODO finalize something here if needed
        // ...
    }

    /**
     * Metodo chamado para cada linha que entra no step
     */
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
            throws KettleException
    {

        DataCubeStepMeta meta = (DataCubeStepMeta) smi;
        DataCubeStepData data = (DataCubeStepData) sdi;

        // Obtem linha do fluxo de entrada e termina caso nao haja mais entrada
        Object[] row = getRow();
        if (row == null)
        { // N�o h� mais linhas de dados
            setOutputDone();
            return false;
        }

        if (first)
        { // Executa apenas uma vez. Variavel first definida na superclasse
            first = false;

            // Obtem todas as colunas at� o step anterior.
            // Chamar apenas apos chamar getRow()
            RowMetaInterface rowMeta = getInputRowMeta();
            data.outputRowMeta = rowMeta.clone();

            // Adiciona os metadados do step atual
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

            // TODO Outras operacoes que devem ser executadas apenas uma vez
            DataTable<String> table = meta.getMapTable();

            //Prefixos
            String prefix = meta.getPrefixes().toString();
            prefix = prefix.replace("[","");
            prefix = prefix.replace(", "," <");
            prefix = prefix.replace("]",">. ");
            prefix = prefix.replace("<@","@");
            prefix = prefix.replace(">. >.",">.");
            prefix = prefix.replace(" <>.  ","");
            prefix = prefix.replace(".  ","."+System.getProperty("line.separator")); 
            
            putOutRow(row, meta, data, prefix);
            
            String prefix_base = prefix.substring(prefix.indexOf(" ")+1, prefix.indexOf("."+System.getProperty("line.separator")));
            
            putOutRow(row, meta, data, "");
            putOutRow(row, meta, data, prefix_base+" a owl:Ontology ;");
            putOutRow(row, meta, data, "	rdfs:label \"Example DataCube Knowledge Base\" ;");
            putOutRow(row, meta, data, "	dc:description \"This knowledgebase contains one Data Structure Definition with one Data Set. This Data Set has a couple of Components and Observations.\" .");
            putOutRow(row, meta, data, "");
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "# Data Structure Definitions");
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "");
            putOutRow(row, meta, data, "ex:dsd a cube:DataStructureDefinition ;");
            putOutRow(row, meta, data, "	rdfs:label \"A Data Structure Definition\"@en ;");
            putOutRow(row, meta, data, "	rdfs:comment \"Defines the structure of a DataSet or slice.\" ;");

         
            // Dimension components
            table = meta.getMapTable();
            for (int i = 0; i < table.size(); i++)
            {
            	String dimensionURI = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_URI_FIELD_NAME.name());	
            	if(dimensionURI != null){
            		if (i!=0){ putOutRow(row, meta, data, "	"+"<"+removeSignals(dimensionURI).toLowerCase()+">,");}
            		else {putOutRow(row, meta, data, "	"+"cube:component "+"	"+"<"+removeSignals(dimensionURI).toLowerCase()+">,");}
            	}
            }
            
            // Measure components
            table = meta.getMapTable1();
            for (int i = 0; i < table.size(); i++)
            {
            	String measureURI = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_URI_FIELD_NAME.name());	
            	if(measureURI != null){
            		putOutRow(row, meta, data, "	"+"<"+removeSignals(measureURI).toLowerCase()+">,");
            	}
            }
            
            
            putOutRow(row, meta, data, "	<http://www.w3.org/2001/XMLSchema#string> .");
            putOutRow(row, meta, data, "");
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "# Component Specifications");
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "");

            table = meta.getMapTable();
            for (int i = 0; i < table.size(); i++)
            {
            	String dimensionField = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_DIMENSIONS_FIELD_NAME.name());
            	String uriDimensao = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_URI_FIELD_NAME.name());
            	String labelDimensao = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_LABELS_FIELD_NAME.name());
            	if (dimensionField != null && uriDimensao != null ){
            		putOutRow(row, meta, data, "<"+removeSignals(uriDimensao).toLowerCase()+"> a cube:ComponentSpecification ;");
            		putOutRow(row, meta, data, "	rdfs:label \"" + labelDimensao + "\" ;");
            		String dimensionURIType = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_URI_TYPE_FIELD_NAME.name());
            		if (!dimensionURIType.isEmpty()){
            			putOutRow(row, meta, data, "	owl:sameAs "+ "<" + dimensionURIType + "> ;");
            		}
            		putOutRow(row, meta, data, "	cube:dimension exProp:" + removeSignals(dimensionField).toLowerCase() + ".");
            		putOutRow(row, meta, data, "");
            	}       
            }
            	
            putOutRow(row, meta, data, "<http://www.w3.org/2001/XMLSchema#float> a cube:ComponentSpecification ;");
            putOutRow(row, meta, data, "	rdfs:label \"Component Specification of Unit\" ;");
            putOutRow(row, meta, data, "	cube:attribute exProp:unit .");
            putOutRow(row, meta, data, "");
            
            table = meta.getMapTable1();
            for (int i = 0; i < table.size(); i++)
            {
            	String measureField = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_FIELD_NAME.name());
            	String uriMedida = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_URI_FIELD_NAME.name());
            	String labelMedida = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_LABEL_FIELD_NAME.name());
            	if (uriMedida != null && measureField != null ){
            		putOutRow(row, meta, data, "<"+uriMedida+"> a cube:ComponentSpecification ;");
            		putOutRow(row, meta, data, "	rdfs:label \"" + labelMedida + "\" ;");
            		String measureURIType = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_URI_TYPE_FIELD_NAME.name());
            		if (!measureURIType.isEmpty()){
            			putOutRow(row, meta, data, "	owl:sameAs "+ "<" + measureURIType + "> ;");
            		}
            		putOutRow(row, meta, data, "	cube:measure exProp:" + removeSignals(measureField).toLowerCase() + ".");
            		putOutRow(row, meta, data, "");
            	}       
            }
            
            
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "# Data Set");
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "ex:dataset a cube:DataSet ;");
            putOutRow(row, meta, data, "	rdfs:label \"A DataSet\"^^<http://www.w3.org/2001/XMLSchema#string> ;");
            putOutRow(row, meta, data, "	rdfs:comment \"Represents a collection of observations and conforming to some common dimensional structure.\" ;");
            putOutRow(row, meta, data, "	cube:structure ex:dsd .");
            putOutRow(row, meta, data, "");
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "# Dimensions, Unit and Measure");
            putOutRow(row, meta, data, "#");

            table = meta.getMapTable();
            for (int i = 0; i < table.size(); i++)
            {
            	String dimensionField = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_DIMENSIONS_FIELD_NAME.name());
            	String labelDimensao = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_LABELS_FIELD_NAME.name());
            	if (dimensionField != null){    		
            		putOutRow(row, meta, data, "exProp:" + removeSignals(dimensionField).toLowerCase() + " a cube:DimensionProperty ;");
            		putOutRow(row, meta, data, "	rdfs:label \"" + labelDimensao + "\"@en .");
            		putOutRow(row, meta, data, "");
            	}       
            }
            
            putOutRow(row, meta, data, "exProp:unit a cube:AttributeProperty ;");
            putOutRow(row, meta, data, "	rdfs:label \"Unit\" .");
            putOutRow(row, meta, data, "");
            
            table = meta.getMapTable1();
            for (int i = 0; i < table.size(); i++)
            {
            	String measureField = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_FIELD_NAME.name());
            	String labelMedida = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_LABEL_FIELD_NAME.name());
            	if (measureField != null ){            		
            		putOutRow(row, meta, data, "exProp:" + removeSignals(measureField).toLowerCase() + " a cube:MeasureProperty ;");
            		putOutRow(row, meta, data, "	rdfs:label \"" + labelMedida + "\" .");
            		putOutRow(row, meta, data, "");
            	}       
            }
            
            table = meta.getMapTable2();
            
            if (!"".equals(DataCubeStepMeta.Field.MAP_TABLE_HIERARCHY_FIELD_NAME.name())){
            	
                putOutRow(row, meta, data, "#");
                putOutRow(row, meta, data, "# HIERARCHYS");
                putOutRow(row, meta, data, "#");
                
                putOutRow(row, meta, data, ""); 
            
	            for (int i = 0; i < table.size(); i++)
	            {
	            	String hierarquia = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_HIERARCHY_FIELD_NAME.name());
	            	String hierarquiaDe = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_HIERARCHY_DE_FIELD_NAME.name());
	            	String hierarquiaLabel = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_HIERARCHY_LABEL_FIELD_NAME.name());
	            	String hierarquiaPara = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_HIERARCHY_PARA_FIELD_NAME.name());
	            	if (hierarquia != null && hierarquiaDe != null && hierarquiaPara != null ){            		
	            		putOutRow(row, meta, data, "ex:" + removeSignals(hierarquiaDe).toLowerCase() + " a ex:" + removeSignals(hierarquia).toLowerCase() + " ;");
	            		putOutRow(row, meta, data, "	obo:part_of ex:" + removeSignals(hierarquiaPara) + ";");
	            		putOutRow(row, meta, data, "	rdfs:label \"" + hierarquiaLabel + "\"@en .");
	            		putOutRow(row, meta, data, "");
	            	}       
	            }
	            
	      
            }
            
            putOutRow(row, meta, data, "#");
            putOutRow(row, meta, data, "# Data Set 1");
            putOutRow(row, meta, data, "#");
            
            putOutRow(row, meta, data, "");
        }

        // Logica do step: leitura de campos de entrada

        
        // Add data properties
        
		putOutRow(row, meta, data, "ex:obj" + j + " a cube:Observation ;");
        putOutRow(row, meta, data, "	cube:dataSet ex:dataset ;");
        
        DataTable<String> table = meta.getMapTable();
        for (int i = 0; i < table.size(); i++)
        {
        	String dimensionField = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_DIMENSIONS_FIELD_NAME.name());
            String dimension = getInputRowMeta().getString(row, dimensionField, "");
            if (!dimension.startsWith("http")){
            	 putOutRow(row, meta, data, "	exProp:"+removeSignals(dimensionField).toLowerCase()+" " + "\"" + dimension + "\" ;");
            }else{putOutRow(row, meta, data, "	exProp:"+removeSignals(dimensionField).toLowerCase()+" " + "<" + dimension + "> ;");}
        }
        
        putOutRow(row, meta, data, "	exProp:unit \""+ meta.getunity() + "\" ;");
        
        table = meta.getMapTable1();
        for (int i = 0; i < table.size(); i++)
        {
        	String measureField = table.getValue(i,DataCubeStepMeta.Field.MAP_TABLE_MEASURE_FIELD_NAME.name());
            String measure = getInputRowMeta().getString(row, measureField, "");
            putOutRow(row, meta, data, "	exProp:"+removeSignals(measureField).toLowerCase()+" " + "\"" + measure + "\" ;");
        }
        
        putOutRow(row, meta, data, "	rdfs:label \"\" .");
        putOutRow(row, meta, data, "");
       
        j++;

        
        return true;
    }

    private void putOutRow(Object[] inputRow,
            DataCubeStepMeta meta,
            DataCubeStepData data, String temp) throws KettleStepException
    {

        int outputRowPos = 0;
        Object[] outputRow = null;

        // Determina se deve repassar campos de entrada

        outputRow = inputRow;
        outputRowPos = getInputRowMeta().size();
        outputRow = new Object[3];
        
        outputRow = RowDataUtil
                .addValueData(outputRow, outputRowPos++, temp);
        // Coloca linha no fluxo
        putRow(data.outputRowMeta, outputRow);
    }
    

    
	private static String removeSignals(String value)
    {
        if (value != null)
        {
            return value.replaceAll("á", "a").replaceAll("à", "a").replaceAll("ä", "a").replaceAll("ã", "a").
            		replaceAll("ú", "u").replaceAll("ù", "u").replaceAll("é", "e").replaceAll("è", "e").replaceAll("ó", "o").
            		replaceAll("ò", "o").replaceAll("ú", "u").replaceAll("ç", "c").replaceAll("í", "i").replaceAll("ì", "i").
            		replaceAll(" ", "").trim();
        }
        else
        {
            return "";
        }
    }
    
}
